#ifndef WINJECT_FEC_STREAM_REACTOR_HPP
#define WINJECT_FEC_STREAM_REACTOR_HPP

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>

#include <bfc/cv_reactor.hpp>
#include <bfc/event_queue.hpp>
#include <bfc/object_registry.hpp>

namespace winject::fec
{

using stream_reactor = bfc::cv_reactor<>;

/* A counter written on the stream's thread and read from any. */
class stream_counter
{
public:
    void operator++(int)
    {
        v.store(v.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    }
    void operator+=(uint64_t n)
    {
        v.store(v.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
    }
    /* From any thread, concurrently with other writers. */
    void add_shared()
    {
        v.fetch_add(1, std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t load() const
    {
        return v.load(std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> v {0};
};

namespace detail
{

/*
 * A stream's link to its reactor: the stream's input ring registered as a reactor context (the
 * reactor drains it when a push wakes it) and one timer armed at the stream's earliest deadline.
 * Without a reactor (manual mode) the caller runs everything on its own thread and drives the
 * deadlines with poll().
 *
 * Any thread may tear a link down: the reactor walks a copy of its context list unlocked, so the
 * ring leaves the list at once but is kept alive by a callback posted to the reactor, which runs
 * after any walk in flight (or is freed with a stopped reactor; it holds no reference back).
 */
class reactor_link
{
public:
    using time_point = std::chrono::steady_clock::time_point;

    ~reactor_link()
    {
        unbind_input();
        disarm();
    }

    void attach(std::shared_ptr<stream_reactor> r)
    {
        unbind_input();
        disarm();
        reactor = std::move(r);
    }
    /* Registers the input ring: a push wakes the reactor, which calls on_input to drain it. */
    void bind_input(std::shared_ptr<stream_reactor::context> ring,
                    stream_reactor::callback_t                on_input)
    {
        unbind_input();
        if (!reactor || !ring)
        {
            return;
        }
        ring->set_reactor(reactor);
        (void)reactor->add_read_rdy(*ring, std::move(on_input));
        input = std::move(ring);
    }
    /* Any thread. */
    void unbind_input()
    {
        if (input && reactor)
        {
            (void)reactor->remove_read_rdy(*input);
            input->set_reactor(nullptr);
            reactor->wake_up([keep = std::move(input)] {});
        }
        input.reset();
    }
    [[nodiscard]] bool attached() const
    {
        return static_cast<bool>(reactor);
    }
    [[nodiscard]] const std::shared_ptr<stream_reactor>& get() const
    {
        return reactor;
    }
    /* True when the caller may touch the stream directly. */
    [[nodiscard]] bool on_thread() const
    {
        return !reactor || reactor->is_reactor_thread();
    }
    /* Arms the one timer for at (none: disarms). Re-arming for the same deadline keeps the
     * timer and builds nothing; otherwise make_cb(generation) builds the callback, which runs on
     * the reactor thread and first asks fired(generation) whether it is still the armed one. */
    template <typename MakeCb>
    void arm(bool any, time_point at, MakeCb&& make_cb)
    {
        if (!reactor)
        {
            return;
        }
        if (armed && any && at == armed_at)
        {
            return;
        }
        disarm();
        if (!any)
        {
            return;
        }
        const int64_t us = std::max<int64_t>(
            0, std::chrono::duration_cast<std::chrono::microseconds>(
                   at - std::chrono::steady_clock::now())
                   .count());
        generation++;
        timer_id = reactor->get_timer().wait_us(us, make_cb(generation));
        armed = true;
        armed_at = at;
    }
    /* A timer fired: true (and the timer is gone) when it is the armed one. False for one that
     * a re-arm replaced after the reactor had already taken it out to run. */
    bool fired(uint64_t gen)
    {
        if (!armed || gen != generation)
        {
            return false;
        }
        armed = false;
        return true;
    }
    void disarm()
    {
        if (armed && reactor)
        {
            (void)reactor->get_timer().cancel(timer_id);
        }
        armed = false;
    }

private:
    std::shared_ptr<stream_reactor>          reactor;
    std::shared_ptr<stream_reactor::context> input;
    bool                                     armed = false;
    uint64_t                                 generation = 0;
    time_point                               armed_at {};
    stream_reactor::timer_t::timer_id_t      timer_id {};
};

}  // namespace detail

namespace detail
{

/*
 * A stream's input from other threads, in the order it was given: items (datagrams, LC bodies) in
 * a bfc lossy ring the reactor drains when a push wakes it, and control calls (configure, reset,
 * ...) beside it. Each takes a ticket; the drain runs every control call ticketed before the next
 * item, so a configure() between two pushes applies between them. Items are dropped (oldest
 * first) when the ring is full, control calls never.
 */
template <typename T, typename Owner>
class stream_input
{
public:
    using control_fn = std::function<void(Owner&)>;
    struct entry
    {
        T        item;
        uint64_t ticket = 0;
    };
    using ring_t = bfc::reactive_circular_lossy_queue<entry>;

    /* At least one: a ring of none would drop every item uncounted. */
    void set_capacity(size_t n)
    {
        n = std::max<size_t>(n, 1);
        ring_->set_capacity(n);
        capacity.store(n, std::memory_order_relaxed);
    }
    [[nodiscard]] const std::shared_ptr<ring_t>& ring() const
    {
        return ring_;
    }
    /* Any thread. False when the ring dropped its oldest item to take this one. */
    bool push(T&& item)
    {
        const size_t cap = capacity.load(std::memory_order_relaxed);
        const uint64_t t = tickets.fetch_add(1, std::memory_order_relaxed);
        return ring_->push(entry {std::move(item), t}) != cap;
    }
    /* Any thread. */
    void add_control(control_fn fn)
    {
        const uint64_t t = tickets.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lg(mtx);
        controls.push_back({t, std::move(fn)});
    }
    /* Stream thread: control calls and items in ticket order. */
    template <typename F>
    void drain(Owner& owner, F&& on_item)
    {
        for (;;)
        {
            std::optional<entry> e = ring_->pop();
            run_controls(owner, e ? e->ticket : UINT64_MAX);
            if (!e)
            {
                return;
            }
            on_item(std::move(e->item));
        }
    }

private:
    void run_controls(Owner& owner, uint64_t before)
    {
        for (;;)
        {
            control_fn fn;
            {
                std::lock_guard<std::mutex> lg(mtx);
                if (controls.empty() || controls.front().first >= before)
                {
                    return;
                }
                fn = std::move(controls.front().second);
                controls.pop_front();
            }
            fn(owner);
        }
    }

    std::shared_ptr<ring_t>                        ring_ = std::make_shared<ring_t>();
    std::atomic<size_t>                            capacity {0};
    std::atomic<uint64_t>                          tickets {0};
    std::mutex                                     mtx;
    std::deque<std::pair<uint64_t, control_fn>>    controls;
};

}  // namespace detail

/* The reactor a stream runs on, by its name in bfc::cv_reactor_registry. */
inline std::shared_ptr<stream_reactor> find_stream_reactor(std::string_view name)
{
    return bfc::cv_reactor_registry::get_instance().get(name);
}

}  // namespace winject::fec

#endif  // WINJECT_FEC_STREAM_REACTOR_HPP

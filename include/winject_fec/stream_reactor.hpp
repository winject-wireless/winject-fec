#ifndef WINJECT_FEC_STREAM_REACTOR_HPP
#define WINJECT_FEC_STREAM_REACTOR_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

#include <bfc/cv_reactor.hpp>
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
 * A stream's link to its reactor: one timer, armed at the stream's earliest deadline, and
 * posting calls made off the reactor thread onto it. Without a reactor (manual mode) the caller
 * runs everything on its own thread and drives the deadlines with poll().
 */
class reactor_link
{
public:
    using time_point = std::chrono::steady_clock::time_point;

    ~reactor_link()
    {
        disarm();
    }

    void attach(std::shared_ptr<stream_reactor> r)
    {
        disarm();
        reactor = std::move(r);
    }
    [[nodiscard]] bool attached() const
    {
        return static_cast<bool>(reactor);
    }
    /* True when the caller may touch the stream directly. */
    [[nodiscard]] bool on_thread() const
    {
        return !reactor || reactor->is_reactor_thread();
    }
    /* Runs fn on the reactor thread (now when already there or in manual mode). */
    void run(std::function<void()> fn)
    {
        if (on_thread())
        {
            fn();
        }
        else
        {
            reactor->wake_up(std::move(fn));
        }
    }
    /* Arms the one timer for at (none: disarms); cb runs on the reactor thread. Re-arming for
     * the same deadline keeps the timer. */
    void arm(bool any, time_point at, std::function<void()> cb)
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
        timer_id = reactor->get_timer().wait_us(us, std::move(cb));
        armed = true;
        armed_at = at;
    }
    /* The armed timer fired: it is gone from the reactor. */
    void fired()
    {
        armed = false;
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
    std::shared_ptr<stream_reactor> reactor;
    bool                            armed = false;
    time_point                      armed_at {};
    stream_reactor::timer_t::timer_id_t timer_id {};
};

}  // namespace detail

/* The reactor a stream runs on, by its name in bfc::cv_reactor_registry. */
inline std::shared_ptr<stream_reactor> find_stream_reactor(std::string_view name)
{
    return bfc::cv_reactor_registry::get_instance().get(name);
}

}  // namespace winject::fec

#endif  // WINJECT_FEC_STREAM_REACTOR_HPP

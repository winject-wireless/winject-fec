add_executable(winject_fec_tests
    fec_test.cpp
    spread_test.cpp
    stream_test.cpp
    rs_encode_scalar_test.cpp
)
target_link_libraries(winject_fec_tests PRIVATE winject_fec winject_fec_isal GTest::gtest_main)
target_compile_options(winject_fec_tests PRIVATE -Wall -Wextra)
include(GoogleTest)
gtest_discover_tests(winject_fec_tests DISCOVERY_MODE PRE_TEST)

# The full path on real reactor threads (seconds, not milliseconds).
add_executable(winject_fec_path_tests path_test.cpp)
target_link_libraries(winject_fec_path_tests PRIVATE winject_fec GTest::gtest_main)
target_compile_options(winject_fec_path_tests PRIVATE -Wall -Wextra)
gtest_discover_tests(winject_fec_path_tests DISCOVERY_MODE PRE_TEST)

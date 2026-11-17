add_executable(winject_fec_tests
    fec_test.cpp
    spread_test.cpp
    rs_encode_scalar_test.cpp
)
target_link_libraries(winject_fec_tests PRIVATE winject_fec winject_fec_isal GTest::gtest_main)
target_compile_options(winject_fec_tests PRIVATE -Wall -Wextra)
include(GoogleTest)
gtest_discover_tests(winject_fec_tests DISCOVERY_MODE PRE_TEST)

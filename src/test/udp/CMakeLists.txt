include_directories(${GLIB_INCLUDE_DIRS})
link_libraries(${GLIB_LIBRARIES})

add_executable(test-udp test_udp.c)
add_linux_tests(BASENAME udp COMMAND sh -c "\
    rm -f udp-fifo && mkfifo udp-fifo && ../shadow-test-launcher test-udp client localhost:0 udp-fifo : test-udp server localhost:0 udp-fifo \
    "
)
add_shadow_tests(BASENAME udp)
add_shadow_tests(BASENAME udp-distributed ARGS --distributed-shard-count 2 --use-new-tcp true)
add_shadow_tests(
    BASENAME udp-distributed-3
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed.yaml
    ARGS --distributed-shard-count 3 --use-new-tcp true
)
add_shadow_tests(
    BASENAME udp-distributed-partition
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-partition.yaml
    ARGS --distributed-shard-count 4 --use-new-tcp true
         --distributed-partition-file ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-partition.partitions.yaml
)
add_shadow_tests(
    BASENAME udp-distributed-large-partition
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-large-partition.yaml
    ARGS --distributed-shard-count 4 --use-new-tcp true
         --distributed-partition-file ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-large-partition.partitions.yaml
)
add_test(
    NAME udp-distributed-large-partition-stats-shadow
    COMMAND ${CMAKE_COMMAND}
        -D RUN_DIR=${CMAKE_CURRENT_BINARY_DIR}/udp-distributed-large-partition-shadow.data
        -P ${CMAKE_CURRENT_SOURCE_DIR}/udp_distributed_large_partition_stats_check.cmake
)
set_tests_properties(udp-distributed-large-partition-stats-shadow
    PROPERTIES DEPENDS udp-distributed-large-partition-shadow)
add_shadow_tests(
    BASENAME udp-distributed-determinism-a
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed.yaml
    ARGS --distributed-shard-count 2 --use-new-tcp true
)
add_shadow_tests(
    BASENAME udp-distributed-determinism-b
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed.yaml
    ARGS --distributed-shard-count 2 --use-new-tcp true
)
add_test(
    NAME udp-distributed-determinism-compare-shadow
    COMMAND ${CMAKE_COMMAND}
        -D RUN_A=${CMAKE_CURRENT_BINARY_DIR}/udp-distributed-determinism-a-shadow.data
        -D RUN_B=${CMAKE_CURRENT_BINARY_DIR}/udp-distributed-determinism-b-shadow.data
        -P ${CMAKE_CURRENT_SOURCE_DIR}/udp_distributed_determinism_compare.cmake
)
set_tests_properties(udp-distributed-determinism-compare-shadow
    PROPERTIES DEPENDS "udp-distributed-determinism-a-shadow;udp-distributed-determinism-b-shadow")

add_executable(test-udp-uniprocess test_udp_uniprocess.c)
add_linux_tests(BASENAME udp-uniprocess COMMAND test-udp-uniprocess)
add_shadow_tests(BASENAME udp-uniprocess)

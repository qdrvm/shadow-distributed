include_directories(${GLIB_INCLUDE_DIRS})
link_libraries(${GLIB_LIBRARIES})
add_executable(test-tcp test_tcp.c)

foreach(BlockingMode blocking nonblocking-poll nonblocking-select nonblocking-epoll iov)
    add_linux_tests(
        BASENAME tcp-${BlockingMode}-loopback
        COMMAND ${CMAKE_SOURCE_DIR}/src/test/launch_with_unused_ports.py ../shadow-test-launcher test-tcp ${BlockingMode}
            server localhost @PORT@ : test-tcp ${BlockingMode} client localhost @PORT@
    )
    foreach(Network loopback lossless lossy)
        # the iov test isn't written in a way that supports lossy networks (it
        # assumes that it can read all bytes at once, and doesn't have the
        # logic to wait for the rest of the bytes after one readv call)
        if("${BlockingMode}" STREQUAL iov AND "${Network}" STREQUAL lossy)
            # TODO: support lossy iov tests
            continue()
        endif()

        add_shadow_tests(BASENAME tcp-${BlockingMode}-${Network})

        if(NOT "${Network}" STREQUAL lossy)
            add_shadow_tests(BASENAME tcp-${BlockingMode}-${Network}-new-tcp
                             SHADOW_CONFIG "${CMAKE_CURRENT_SOURCE_DIR}/tcp-${BlockingMode}-${Network}.yaml"
                             ARGS --use-new-tcp true)
        endif()
    endforeach()
endforeach()

add_shadow_tests(BASENAME tcp-distributed ARGS --distributed-shard-count 2 --use-new-tcp true)
add_shadow_tests(
    BASENAME tcp-distributed-determinism-a
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/tcp-distributed.yaml
    ARGS --distributed-shard-count 2 --use-new-tcp true
)
add_shadow_tests(
    BASENAME tcp-distributed-determinism-b
    SHADOW_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/tcp-distributed.yaml
    ARGS --distributed-shard-count 2 --use-new-tcp true
)
add_test(
    NAME tcp-distributed-determinism-compare-shadow
    COMMAND ${CMAKE_COMMAND}
        -D RUN_A=${CMAKE_CURRENT_BINARY_DIR}/tcp-distributed-determinism-a-shadow.data
        -D RUN_B=${CMAKE_CURRENT_BINARY_DIR}/tcp-distributed-determinism-b-shadow.data
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tcp_distributed_determinism_compare.cmake
)
set_tests_properties(tcp-distributed-determinism-compare-shadow
    PROPERTIES DEPENDS "tcp-distributed-determinism-a-shadow;tcp-distributed-determinism-b-shadow")

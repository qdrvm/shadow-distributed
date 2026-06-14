# Distributed (MPI) tests for Shadow.
# These tests require SHADOW_USE_MPI=ON and a working MPI installation.
# They are only registered when SHADOW_USE_MPI is enabled.

if(SHADOW_USE_MPI STREQUAL ON AND SHADOW_TEST STREQUAL ON)
    # Two-rank MPI UDP test: hosta on rank 0, hostb on rank 1.
    # Uses modulo partition (default when no partition file is given).
    add_test(
        NAME udp-distributed-mpi-shadow
        COMMAND sh -c "\
            rm -rf udp-distributed-mpi-shadow.data.shard-* \
            && mpirun -np 2 \
                --oversubscribe \
                ${CMAKE_BINARY_DIR}/src/main/shadow \
                --data-directory=udp-distributed-mpi-shadow.data \
                --log-level=info \
                --use-cpu-pinning false \
                --strace-logging-mode standard \
                --parallelism 1 \
                --report-errors-to-stderr false \
                --distributed-shard-count 2 \
                --use-new-tcp true \
                ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-mpi.yaml \
            "
    )
    set_property(TEST udp-distributed-mpi-shadow PROPERTY
        ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
    set_property(TEST udp-distributed-mpi-shadow PROPERTY
        FAIL_REGULAR_EXPRESSION "Memory leak detected")
    set_property(TEST udp-distributed-mpi-shadow APPEND PROPERTY LABELS shadow distributed mpi)
    # MPI tests take longer due to launch overhead
    set_tests_properties(udp-distributed-mpi-shadow PROPERTIES TIMEOUT 120)

    # Four-rank MPI UDP test
    add_test(
        NAME udp-distributed-mpi-4-shadow
        COMMAND sh -c "\
            rm -rf udp-distributed-mpi-4-shadow.data.shard-* \
            && mpirun -np 4 \
                --oversubscribe \
                ${CMAKE_BINARY_DIR}/src/main/shadow \
                --data-directory=udp-distributed-mpi-4-shadow.data \
                --log-level=info \
                --use-cpu-pinning false \
                --strace-logging-mode standard \
                --parallelism 1 \
                --report-errors-to-stderr false \
                --distributed-shard-count 4 \
                --use-new-tcp true \
                ${CMAKE_CURRENT_SOURCE_DIR}/udp-distributed-mpi.yaml \
            "
    )
    set_property(TEST udp-distributed-mpi-4-shadow PROPERTY
        ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
    set_property(TEST udp-distributed-mpi-4-shadow PROPERTY
        FAIL_REGULAR_EXPRESSION "Memory leak detected")
    set_property(TEST udp-distributed-mpi-4-shadow APPEND PROPERTY LABELS shadow distributed mpi)
    set_tests_properties(udp-distributed-mpi-4-shadow PROPERTIES TIMEOUT 120)
endif()

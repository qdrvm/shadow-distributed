# Distributed (MPI) tests for Shadow.
# These tests require SHADOW_USE_MPI=ON and a working MPI installation.
# They are only registered when SHADOW_USE_MPI is enabled.

if(SHADOW_USE_MPI STREQUAL ON AND SHADOW_TEST STREQUAL ON)
    # Generate config with full binary path at configure time.
    set(UDP_MPI_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/udp-distributed-mpi-generated.yaml")
    file(WRITE "${UDP_MPI_CONFIG}"
        "general:\n"
        "  stop_time: 10\n"
        "network:\n"
        "  graph:\n"
        "    type: 1_gbit_switch\n"
        "experimental:\n"
        "  use_new_tcp: true\n"
        "hosts:\n"
        "  hosta:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: server 0.0.0.0:5678\n"
        "      start_time: 1\n"
        "  hostb:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: client hosta:5678\n"
        "      start_time: 2\n"
    )

    # Two-rank MPI UDP test
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
                ${UDP_MPI_CONFIG} \
            "
    )
    set_property(TEST udp-distributed-mpi-shadow PROPERTY
        ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
    set_property(TEST udp-distributed-mpi-shadow PROPERTY
        FAIL_REGULAR_EXPRESSION "Memory leak detected")
    set_property(TEST udp-distributed-mpi-shadow APPEND PROPERTY LABELS shadow distributed mpi)
    set_tests_properties(udp-distributed-mpi-shadow PROPERTIES TIMEOUT 120)

    # Four-rank MPI UDP test — needs 4 hosts + host pairs so each rank gets work.
    set(UDP_MPI_4_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/udp-distributed-mpi-4-generated.yaml")
    file(WRITE "${UDP_MPI_4_CONFIG}"
        "general:\n"
        "  stop_time: 10\n"
        "network:\n"
        "  graph:\n"
        "    type: 1_gbit_switch\n"
        "experimental:\n"
        "  use_new_tcp: true\n"
        "hosts:\n"
        "  hosta:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: server 0.0.0.0:5678\n"
        "      start_time: 1\n"
        "  hostb:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: client hosta:5678\n"
        "      start_time: 2\n"
        "  hostc:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: server 0.0.0.0:5679\n"
        "      start_time: 1\n"
        "  hostd:\n"
        "    network_node_id: 0\n"
        "    processes:\n"
        "    - path: ${CMAKE_BINARY_DIR}/src/test/udp/test-udp\n"
        "      args: client hostc:5679\n"
        "      start_time: 2\n"
    )
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
                ${UDP_MPI_4_CONFIG} \
            "
    )
    set_property(TEST udp-distributed-mpi-4-shadow PROPERTY
        ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
    set_property(TEST udp-distributed-mpi-4-shadow PROPERTY
        FAIL_REGULAR_EXPRESSION "Memory leak detected")
    set_property(TEST udp-distributed-mpi-4-shadow APPEND PROPERTY LABELS shadow distributed mpi)
    set_tests_properties(udp-distributed-mpi-4-shadow PROPERTIES TIMEOUT 120)
endif()

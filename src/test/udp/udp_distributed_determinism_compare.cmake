macro(NORMALIZED_DIFF_CHECK FILE1 FILE2)
    file(READ ${FILE1} CONTENT1)
    file(READ ${FILE2} CONTENT2)

    string(REGEX REPLACE "# random seed: [^\n]*\n" "" CONTENT1 "${CONTENT1}")
    string(REGEX REPLACE "# random seed: [^\n]*\n" "" CONTENT2 "${CONTENT2}")
    string(REGEX REPLACE "# slow test [^\n]*\n" "" CONTENT1 "${CONTENT1}")
    string(REGEX REPLACE "# slow test [^\n]*\n" "" CONTENT2 "${CONTENT2}")

    if(NOT "${CONTENT1}" STREQUAL "${CONTENT2}")
        message(STATUS "Normalized contents differ for '${FILE1}' and '${FILE2}'")
        message(FATAL_ERROR "Differences found; test failed")
    endif()
endmacro()

normalized_diff_check(
    ${RUN_A}.shard-0/hosts/localnode/test-udp.1000.stdout
    ${RUN_B}.shard-0/hosts/localnode/test-udp.1000.stdout
)
normalized_diff_check(
    ${RUN_A}.shard-0/hosts/localnode/test-udp.1001.stdout
    ${RUN_B}.shard-0/hosts/localnode/test-udp.1001.stdout
)
normalized_diff_check(
    ${RUN_A}.shard-0/hosts/testserver/test-udp.1000.stdout
    ${RUN_B}.shard-0/hosts/testserver/test-udp.1000.stdout
)
normalized_diff_check(
    ${RUN_A}.shard-1/hosts/testclient/test-udp.1000.stdout
    ${RUN_B}.shard-1/hosts/testclient/test-udp.1000.stdout
)

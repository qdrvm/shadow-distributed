macro(EXEC_DIFF_CHECK FILE1 FILE2)
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E compare_files ${FILE1} ${FILE2}
        RESULT_VARIABLE RESULT
        OUTPUT_VARIABLE STDOUTPUT
        ERROR_VARIABLE STDERROR)
    message(STATUS "Diff returned ${RESULT} for '${FILE1}' and '${FILE2}'")
    if(RESULT)
        message(STATUS "Diff stdout is: ${STDOUTPUT}")
        message(STATUS "Diff stderr is: ${STDERROR}")
        message(FATAL_ERROR "Differences found; test failed")
    endif()
endmacro()

exec_diff_check(
    ${RUN_A}.shard-0/hosts/lossless.tcpclient.echo/test-tcp.1000.stdout
    ${RUN_B}.shard-0/hosts/lossless.tcpclient.echo/test-tcp.1000.stdout
)
exec_diff_check(
    ${RUN_A}.shard-1/hosts/lossless.tcpserver.echo/test-tcp.1000.stdout
    ${RUN_B}.shard-1/hosts/lossless.tcpserver.echo/test-tcp.1000.stdout
)

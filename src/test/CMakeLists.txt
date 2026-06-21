## testing for the shadow simulator

add_compile_options(-ggdb)
add_compile_options(-fno-omit-frame-pointer)
add_compile_options(-Wreturn-type)
add_compile_options(-Wswitch)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    # we only check for unused functions when builing in debug mode since some
    # functions are only called when logging, which can be #ifdef'd out in
    # release mode
    add_compile_options(-Wunused-function)
endif()

## these are common flags that are needed for shadow plugins
add_cflags("-fPIC -fno-inline -fno-strict-aliasing -U_FORTIFY_SOURCE -Wno-unused-command-line-argument")
add_cflags(-std=gnu99)
add_definitions(-fPIC -g3 -DDEBUG -D_GNU_SOURCE)

## ensure that the Host LLVM plugin is built first
#get_property(LLVMHoistGlobalsPATH TARGET LLVMHoistGlobals PROPERTY LOCATION)

# Fake target to build all extra tests
add_custom_target(extra_tests)

## a helper program for tests needing multiple processes
add_executable(shadow-test-launcher test_launcher.c test_launcher_common.c)
add_executable(shadow-test-launcher-fail test_launcher_fail.c test_launcher_common.c)

## === Helper macros for consistently defining tests. ===
## Tests that need more features should use add_test directly.

## example: add_linux_tests(BASENAME bind COMMAND test_bind)
## will create one test named bind-linux
macro(add_linux_tests)
   cmake_parse_arguments(LINUX_TEST "" "BASENAME" "COMMAND;CONFIGURATIONS;PROPERTIES" ${ARGN})
   if(DEFINED LINUX_TEST_UNPARSED_ARGUMENTS)
      message(FATAL_ERROR "Unrecognized arguments: ${LINUX_TEST_UNPARSED_ARGUMENTS}")
   endif()

   set(LINUX_TEST_NAME ${LINUX_TEST_BASENAME}-linux)
   if(DEFINED LINUX_TEST_CONFIGURATIONS)
      add_test(
         NAME ${LINUX_TEST_NAME}
         COMMAND ${LINUX_TEST_COMMAND}
         CONFIGURATIONS ${LINUX_TEST_CONFIGURATIONS}
         )
   else()
      add_test(
         NAME ${LINUX_TEST_NAME}
         COMMAND ${LINUX_TEST_COMMAND})
   endif()
   set_property(TEST ${LINUX_TEST_NAME} PROPERTY ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
   if(DEFINED LINUX_TEST_PROPERTIES)
      set_tests_properties(${LINUX_TEST_NAME} PROPERTIES ${LINUX_TEST_PROPERTIES})
   endif()
   set_property(TEST ${LINUX_TEST_NAME} APPEND PROPERTY LABELS linux)
endmacro()

## example: add_shadow_tests(BASENAME bind LOGLEVEL debug ARGS --pin-cpus)
## will create a test named bind-shadow
macro(add_shadow_tests)
   cmake_parse_arguments(SHADOW_TEST "" "BASENAME;LOGLEVEL;SHADOW_CONFIG;POST_CMD;EXPECT_ERROR" "ARGS;CONFIGURATIONS;PROPERTIES" ${ARGN})
   if(DEFINED SHADOW_TEST_UNPARSED_ARGUMENTS)
      message(FATAL_ERROR "Unrecognized arguments: ${SHADOW_TEST_UNPARSED_ARGUMENTS}")
   endif()

   if(NOT DEFINED SHADOW_TEST_LOGLEVEL)
      if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
         set(SHADOW_TEST_LOGLEVEL "trace")
      else()
         set(SHADOW_TEST_LOGLEVEL "info")
      endif()
   endif()

   if(NOT DEFINED SHADOW_TEST_SHADOW_CONFIG)
       set(SHADOW_TEST_SHADOW_CONFIG "${CMAKE_CURRENT_SOURCE_DIR}/${SHADOW_TEST_BASENAME}.yaml")
   endif()

   if(NOT DEFINED SHADOW_TEST_EXPECT_ERROR)
      set(SHADOW_TEST_EXPECT_ERROR FALSE)
   endif()

   if(SHADOW_TEST_EXPECT_ERROR)
      set(INVERT_ERROR_CODE "!")
   else()
      set(INVERT_ERROR_CODE "")
   endif()

   # If cpu pinning is not explicitly set, we disable it so we avoid pinning
   # multiple tests that should be running in parallel to the same exact CPUs.
   if(NOT "${SHADOW_TEST_ARGS}" MATCHES ".*--use-cpu-pinning.*")
      list(APPEND SHADOW_TEST_ARGS "--use-cpu-pinning" "false")
   endif()

   # If strace logging is not explicitly set, we enable it
   if(NOT "${SHADOW_TEST_ARGS}" MATCHES ".*--strace-logging-mode.*")
      list(APPEND SHADOW_TEST_ARGS "--strace-logging-mode" "standard")
   endif()

   # If parallelism is not explicitly set, we use a single thread
   if(NOT "${SHADOW_TEST_ARGS}" MATCHES ".*--parallelism.*")
      list(APPEND SHADOW_TEST_ARGS "--parallelism" "1")
   endif()

   # If mirroring errors to stderr is not explicitly set, we disable it, since
   # tests are normally run with stdout and stderr merged into one stream,
   # causing duplication.
   if(NOT "${SHADOW_TEST_ARGS}" MATCHES ".*--report-errors-to-stderr.*")
      list(APPEND SHADOW_TEST_ARGS "--report-errors-to-stderr" "false")
   endif()

   string(REPLACE ";" " " SHADOW_TEST_ARGS "${SHADOW_TEST_ARGS}")

   set(SHADOW_TEST_NAME ${SHADOW_TEST_BASENAME}-shadow)

   if(DEFINED SHADOW_TEST_POST_CMD)
      set(POST_CMD "cd ${SHADOW_TEST_NAME}.data && ${SHADOW_TEST_POST_CMD}")
   else()
      set(POST_CMD "true")
   endif()

   set(SHADOW_TEST_COMMAND sh -c "\
       rm -rf ${SHADOW_TEST_NAME}.data ${SHADOW_TEST_NAME}.data.shard-* \
      && ${INVERT_ERROR_CODE} ${CMAKE_BINARY_DIR}/src/main/shadow \
      --data-directory=${SHADOW_TEST_NAME}.data \
      --log-level=${SHADOW_TEST_LOGLEVEL} \
      ${SHADOW_TEST_ARGS} \
      ${SHADOW_TEST_SHADOW_CONFIG} \
      && (${POST_CMD}) \
      "
   )

   if(DEFINED SHADOW_TEST_CONFIGURATIONS)
      add_test(
         NAME ${SHADOW_TEST_NAME}
         COMMAND ${SHADOW_TEST_COMMAND}
         CONFIGURATIONS ${SHADOW_TEST_CONFIGURATIONS}
      )
   else()
      add_test(
         NAME ${SHADOW_TEST_NAME}
         COMMAND ${SHADOW_TEST_COMMAND}
      )
   endif()

   # check that we didn't leak any reference-counted objects
   set(FAIL_REGEX "Memory leak detected")

   set_property(TEST ${SHADOW_TEST_NAME} PROPERTY ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")

   set_property(TEST ${SHADOW_TEST_NAME} PROPERTY FAIL_REGULAR_EXPRESSION ${FAIL_REGEX})

   if(DEFINED SHADOW_TEST_PROPERTIES)
      set_tests_properties(${SHADOW_TEST_NAME} PROPERTIES ${SHADOW_TEST_PROPERTIES})
   endif()

   set_property(TEST ${SHADOW_TEST_NAME} APPEND PROPERTY LABELS shadow)
endmacro()

macro(add_mpi_shadow_tests)
   cmake_parse_arguments(MPI_SHADOW_TEST "" "BASENAME;NPROCS;SHADOW_CONFIG;POST_CMD;WORKING_DIRECTORY" "ARGS;PROPERTIES" ${ARGN})
   if(DEFINED MPI_SHADOW_TEST_UNPARSED_ARGUMENTS)
      message(FATAL_ERROR "Unrecognized arguments: ${MPI_SHADOW_TEST_UNPARSED_ARGUMENTS}")
   endif()

   if(SHADOW_USE_MPI STREQUAL ON)
      if(NOT DEFINED MPI_SHADOW_TEST_SHADOW_CONFIG)
         set(MPI_SHADOW_TEST_SHADOW_CONFIG "${CMAKE_CURRENT_SOURCE_DIR}/${MPI_SHADOW_TEST_BASENAME}.yaml")
      endif()

      if(NOT DEFINED MPI_SHADOW_TEST_WORKING_DIRECTORY)
         set(MPI_SHADOW_TEST_WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
      endif()

      if(NOT DEFINED MPI_SHADOW_TEST_POST_CMD)
         set(POST_CMD "true")
      else()
         set(POST_CMD "${MPI_SHADOW_TEST_POST_CMD}")
      endif()

      if(NOT "${MPI_SHADOW_TEST_ARGS}" MATCHES ".*--use-cpu-pinning.*")
         list(APPEND MPI_SHADOW_TEST_ARGS "--use-cpu-pinning" "false")
      endif()
      if(NOT "${MPI_SHADOW_TEST_ARGS}" MATCHES ".*--strace-logging-mode.*")
         list(APPEND MPI_SHADOW_TEST_ARGS "--strace-logging-mode" "standard")
      endif()
      if(NOT "${MPI_SHADOW_TEST_ARGS}" MATCHES ".*--parallelism.*")
         list(APPEND MPI_SHADOW_TEST_ARGS "--parallelism" "1")
      endif()
      if(NOT "${MPI_SHADOW_TEST_ARGS}" MATCHES ".*--progress.*")
         list(APPEND MPI_SHADOW_TEST_ARGS "--progress" "false")
      endif()
      if(NOT "${MPI_SHADOW_TEST_ARGS}" MATCHES ".*--report-errors-to-stderr.*")
         list(APPEND MPI_SHADOW_TEST_ARGS "--report-errors-to-stderr" "false")
      endif()

      list(APPEND MPI_SHADOW_TEST_ARGS
         "--distributed-shard-count" "${MPI_SHADOW_TEST_NPROCS}"
         "--use-new-tcp" "true"
      )

      string(REPLACE ";" " " MPI_SHADOW_TEST_ARGS "${MPI_SHADOW_TEST_ARGS}")
      string(REPLACE ";" " " MPIEXEC_PREFLAGS_STRING "${MPIEXEC_PREFLAGS}")
      string(REPLACE ";" " " MPIEXEC_POSTFLAGS_STRING "${MPIEXEC_POSTFLAGS}")

      set(MPI_SHADOW_TEST_NAME ${MPI_SHADOW_TEST_BASENAME}-shadow)
      set(MPI_SHADOW_TEST_DATA_DIR ${CMAKE_CURRENT_BINARY_DIR}/${MPI_SHADOW_TEST_NAME}.data)
      set(MPI_SHADOW_TEST_COMMAND sh -c "\
         rm -rf ${MPI_SHADOW_TEST_DATA_DIR} ${MPI_SHADOW_TEST_DATA_DIR}.shard-* \
         && ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPI_SHADOW_TEST_NPROCS} ${MPIEXEC_PREFLAGS_STRING} \
            ${CMAKE_BINARY_DIR}/src/main/shadow ${MPIEXEC_POSTFLAGS_STRING} \
            --data-directory=${MPI_SHADOW_TEST_DATA_DIR} \
            --log-level=info \
            ${MPI_SHADOW_TEST_ARGS} \
            ${MPI_SHADOW_TEST_SHADOW_CONFIG} \
         && (${POST_CMD}) \
         "
      )

      add_test(NAME ${MPI_SHADOW_TEST_NAME} COMMAND ${MPI_SHADOW_TEST_COMMAND})
      set_property(TEST ${MPI_SHADOW_TEST_NAME} PROPERTY WORKING_DIRECTORY ${MPI_SHADOW_TEST_WORKING_DIRECTORY})
      set_property(TEST ${MPI_SHADOW_TEST_NAME} PROPERTY ENVIRONMENT "RUST_BACKTRACE=1;G_DEBUG=fatal-criticals")
      set_property(TEST ${MPI_SHADOW_TEST_NAME} PROPERTY FAIL_REGULAR_EXPRESSION "Memory leak detected")
      set_property(TEST ${MPI_SHADOW_TEST_NAME} APPEND PROPERTY LABELS shadow mpi)
      if(DEFINED MPI_SHADOW_TEST_PROPERTIES)
         set_tests_properties(${MPI_SHADOW_TEST_NAME} PROPERTIES ${MPI_SHADOW_TEST_PROPERTIES})
      endif()
   endif()
endmacro()
## === end test helper macros ===

add_subdirectory(bindc)
add_subdirectory(capabilities)
add_subdirectory(cli)
add_subdirectory(clone)
add_subdirectory(close_range)
add_subdirectory(compressed-graph)
add_subdirectory(config)
add_subdirectory(cpp)
add_subdirectory(determinism)
add_subdirectory(distributed)
add_subdirectory(dup)
add_subdirectory(environment)
add_subdirectory(epoll)
add_subdirectory(eventfd)
add_subdirectory(examples)
add_subdirectory(exit)
add_subdirectory(file)
add_subdirectory(futex)
add_subdirectory(golang)
add_subdirectory(ifaddrs)
add_subdirectory(memory)
add_subdirectory(netlink)
add_subdirectory(phold)
add_subdirectory(pipe)
add_subdirectory(poll)
add_subdirectory(prctl)
add_subdirectory(random)
add_subdirectory(regression)
add_subdirectory(resolver)
add_subdirectory(resource)
add_subdirectory(sched)
add_subdirectory(sched_affinity)
add_subdirectory(select)
add_subdirectory(signal)
add_subdirectory(sleep)
add_subdirectory(sockbuf)
add_subdirectory(socket)
add_subdirectory(stat)
add_subdirectory(static-bin)
add_subdirectory(stdio)
add_subdirectory(sysinfo)
add_subdirectory(tcp)
add_subdirectory(tgen)
add_subdirectory(threads)
add_subdirectory(time)
add_subdirectory(timerfd)
add_subdirectory(tor)
add_subdirectory(udp)
add_subdirectory(unistd)

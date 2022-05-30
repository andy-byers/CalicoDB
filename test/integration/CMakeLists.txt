set(INTEGRATION_DIR "${CUB_TEST_DIR}/integration")
set(
    COMMON_FILES
    ${INTEGRATION_DIR}/integration.cpp
    ${INTEGRATION_DIR}/integration.h
)

set(
    RW_TEST_FILES
    ${INTEGRATION_DIR}/test_rw.cpp
    ${COMMON_FILES}
)
build_test(test_rw "${RW_TEST_FILES}")
add_test(
    NAME test_readers_and_writers
    COMMAND test_rw
)

set(
    RECOVERY_TEST_FILES
    ${INTEGRATION_DIR}/test_recovery.cpp
    ${COMMON_FILES}
)
build_test(test_recovery "${RECOVERY_TEST_FILES}")



# The script gen_examples.py concatenates the C++ code blocks into a main function.
# check_refs.py makes sure each internal and external reference exists.
set(WRITE_CPP_BLOCKS "${CMAKE_CURRENT_SOURCE_DIR}/gen_examples.py")
set(CHECK_REFERENCES "${CMAKE_CURRENT_SOURCE_DIR}/check_refs.py")
set(EXAMPLES_SOURCE "${CMAKE_CURRENT_BINARY_DIR}/examples.cpp")
set(README_FILE "${CMAKE_SOURCE_DIR}/doc/design.md")
set(DESIGN_FILE "${CMAKE_SOURCE_DIR}/README.md")
set(DOC_FILE "${CMAKE_SOURCE_DIR}/doc/doc.md")
# Generate and build the example source file.
add_custom_command(OUTPUT "${EXAMPLES_SOURCE}"
        COMMAND python3 "${WRITE_CPP_BLOCKS}" "${DOC_FILE}" "${EXAMPLES_SOURCE}"
        DEPENDS "${WRITE_CPP_BLOCKS}"
        COMMENT "Writing ${EXAMPLES_SOURCE}")
add_executable(doc_examples "${EXAMPLES_SOURCE}")
target_link_libraries(doc_examples calicodb)
# This test just runs the example executable and makes sure it doesn't crash.
add_test(NAME doc_examples
        COMMAND doc_examples)
# This test resolves filesystem and section references in the docs.
add_test(NAME doc_references
        COMMAND python3 "${CHECK_REFERENCES}" "${README_FILE}" "${DOC_FILE}" "${DESIGN_FILE}")


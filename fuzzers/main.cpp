// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Based off of StandaloneFuzzTargetMain.c in libFuzzer.

#include "fuzzer.h"
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

auto main(int argc, const char *argv[]) -> int
{
    const auto run_input = [](const auto *filename) {
        std::fprintf(stderr, "Running: %s\n", filename);

        auto *fp = std::fopen(filename, "r");
        CHECK_TRUE(fp);

        std::fseek(fp, 0, SEEK_END);
        const auto rc = std::ftell(fp);
        CHECK_TRUE(0 <= rc);
        auto file_size = static_cast<size_t>(rc);
        std::fseek(fp, 0, SEEK_SET);

        std::string buffer(file_size, '\0');
        const auto read_size = std::fread(buffer.data(), 1, file_size, fp);
        CHECK_EQ(read_size, file_size);

        std::fclose(fp);

        const auto *data = reinterpret_cast<const uint8_t *>(buffer.data());
        LLVMFuzzerTestOneInput(data, file_size);
        std::fprintf(stderr, "Done:    %s: (%zu bytes)\n", filename, file_size);
    };

    std::fprintf(stderr, "main: running %d inputs\n", argc - 1);

    namespace fs = std::filesystem;

    for (int i = 1; i < argc; ++i) {
        if (fs::is_directory(argv[i])) {
            for (const auto &entry : fs::directory_iterator(argv[i])) {
                run_input(entry.path().c_str());
            }
        } else {
            run_input(argv[i]);
        }
    }
    return 0;
}

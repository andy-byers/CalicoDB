// Based off of StandaloneFuzzTargetMain.c in libFuzzer.

#include "tools.h"
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size);

auto main(int argc, const char *argv[]) -> int
{
    const auto run_input = [](const auto *filename) {
        std::fprintf(stderr, "Running: %s\n", filename);

        auto *fp = std::fopen(filename, "r");
        CHECK_TRUE(fp != nullptr);

        std::fseek(fp, 0, SEEK_END);
        const auto file_size = std::ftell(fp);
        std::fseek(fp, 0, SEEK_SET);

        std::string buffer(file_size, '\0');
        CHECK_EQ(std::fread(buffer.data(), 1, file_size, fp), file_size);

        std::fclose(fp);

        const auto *data = reinterpret_cast<const std::uint8_t *>(buffer.data());
        LLVMFuzzerTestOneInput(data, file_size);
        std::fprintf(stderr, "Done:    %s: (%zu bytes)\n", filename, file_size);
    };

    std::fprintf(stderr, "main: running %d inputs\n", argc - 1);

    namespace fs = std::filesystem;

    for (std::size_t i {1}; i < argc; ++i) {
        if (fs::is_directory(argv[i])) {
            for (const auto &entry : fs::directory_iterator {argv[i]}) {
                run_input(entry.path().c_str());
            }
        } else {
            run_input(argv[i]);
        }
    }
    return 0;
}

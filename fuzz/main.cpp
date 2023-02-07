// Based off of StandaloneFuzzTargetMain.c in libFuzzer.

#include <cassert>
#include <cstdio>
#include <fstream>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const unsigned char *data, size_t size);

int main(int argc, char **argv) {
    std::fprintf(stderr, "main: running %d inputs\n", argc - 1);

    for (std::size_t i {1}; i < argc; ++i) {
        std::fprintf(stderr, "Running: %s\n", argv[i]);
        auto *fp = fopen(argv[i], "r");
        assert(fp != nullptr);

        fseek(fp, 0, SEEK_END);
        const auto file_size = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        std::string buffer(file_size, '\0');
        const auto read_size = fread(buffer.data(), 1, file_size, fp);
        assert(read_size == file_size);

        fclose(fp);

        const auto *data = reinterpret_cast<const std::uint8_t *>(buffer.data());
        LLVMFuzzerTestOneInput(data, file_size);
        fprintf(stderr, "Done:    %s: (%zu bytes)\n", argv[i], file_size);
    }
}

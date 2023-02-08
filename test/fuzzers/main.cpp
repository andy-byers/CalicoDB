// Based off of StandaloneFuzzTargetMain.c in libFuzzer.

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/stat.h>

extern "C" int LLVMFuzzerTestOneInput(const unsigned char *data, size_t size);

int main(int argc, const char *argv[]) {

    const auto run_input = [argv](const auto *filename) {
        std::fprintf(stderr, "Running: %s\n", filename);
        auto *fp = fopen(filename, "r");
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
        fprintf(stderr, "Done:    %s: (%zu bytes)\n", filename, file_size);
    };

    std::fprintf(stderr, "main: running %d inputs\n", argc - 1);

    for (std::size_t i {1}; i < argc; ++i) {
        run_input(argv[i]);
    }
}



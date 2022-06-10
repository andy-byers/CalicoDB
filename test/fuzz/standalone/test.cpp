/*===- StandaloneFuzzTargetMain.c - standalone main() for fuzz targets. ---===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
// This main() function can be linked to a fuzz target (i.e. a library
// that exports LLVMFuzzerTestOneInput() and possibly LLVMFuzzerInitialize())
// instead of libFuzzer. This main() function will not perform any fuzzing
// but will simply feed all input files one by one to the fuzz target.
//
// Use this file to provide reproducers for bugs when linking against libFuzzer
// or other fuzzing engine is undesirable.
//===----------------------------------------------------------------------===*/
#include <vector>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "cub/cub.h"
#include "file/file.h"

extern int LLVMFuzzerTestOneInput(const uint8_t*, Size);

__attribute__((weak)) extern int LLVMFuzzerInitialize(int *argc, char ***argv);

auto main(int argc, char **argv) -> int
{
    fprintf(stderr, "StandaloneFuzzTargetMain: running %d inputs\n", argc - 1);

    if (LLVMFuzzerInitialize)
        LLVMFuzzerInitialize(&argc, &argv);

    for (int i {1}; i < argc; i++) {
        fprintf(stderr, "Running: %s\n", argv[i]);
        ReadOnlyFile file {argv[i], Mode::READ_ONLY, 0666};
        std::string buffer(file.size(), '\x00');
        read_exact(file, _b(buffer));
        LLVMFuzzerTestOneInput(reinterpret_cast<uint8_t*>(buffer.data()), buffer.size());
        fprintf(stderr, "Done:    %s: (%zu bytes)\n", argv[i], buffer.size());
    }
}
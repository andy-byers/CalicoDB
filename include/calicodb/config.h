// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CONFIG_H
#define CALICODB_CONFIG_H

#include <cstdlib>

#ifndef CALICODB_DEFAULT_MALLOC
#define CALICODB_DEFAULT_MALLOC std::malloc
#endif // CALICODB_DEFAULT_MALLOC

#ifndef CALICODB_DEFAULT_REALLOC
#define CALICODB_DEFAULT_REALLOC std::realloc
#endif // CALICODB_DEFAULT_REALLOC

#ifndef CALICODB_DEFAULT_FREE
#define CALICODB_DEFAULT_FREE std::free
#endif // CALICODB_DEFAULT_FREE

namespace calicodb
{

// calicodb/status.h
class Status;

// Global allocator options set by configure(kConfigAllocator, ...). Defaults to the above allocation
// functions (CALICODB_DEFAULT_*()). Allocation function calls are serialized using a global mutex, so
// they need not be thread safe.
struct AllocatorConfig {
    using Malloc = void *(*)(size_t);
    using Realloc = void *(*)(void *, size_t);
    using Free = void (*)(void *);

    Malloc malloc;
    Realloc realloc;
    Free free;
};

struct SyscallConfig {
    const char *name;
    void *syscall;
};

// Specifies a target for configure(). configure() expects a different argument depending on the config
// target.                 Type                    | Details
enum ConfigTarget {    // -------------------------|------------------------------------------------------
    kReplaceAllocator, //  const AllocatorConfig * | Replace the general-purpose allocator
    kRestoreAllocator, //  nullptr                 | Restore the general-purpose allocator to the default
    kReplaceSyscall,   //  const SyscallConfig *   | Replace a system call
    kRestoreSyscall,   //  const char *            | Restore the named syscall to its default
};

// Configure per-process options
// This function is not safe to call from multiple threads simultaneously.
// If `target` is recognized and the configuration option set successfully, an OK status is returned.
// Otherwise, a non-OK status is returned with no side effects.
auto configure(ConfigTarget target, const void *value) -> Status;

} // namespace calicodb

#endif // CALICODB_OPTIONS_H

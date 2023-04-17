// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "status.h"

namespace calicodb
{

class File;
class LogFile;

// CalicoDB storage environment
//
// Handles platform-specific filesystem manipulations and file locking.
class Env
{
public:
    enum OpenMode : int {
        kCreate = 1,

        // These 2 flags are mutually exclusive: only 1 may be set when opening a file.
        kReadOnly = 2,
        kReadWrite = 4,
    };

    // File locking modes and semantics are from SQLite.
    enum LockMode : int {
        kUnlocked,
        kShared,
        kReserved,
        kPending,
        kExclusive,
    };

    // Return a heap-allocated handle to this platform's default Env implementation
    static auto default_env() -> Env *;

    virtual ~Env();
    [[nodiscard]] virtual auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status = 0;
    [[nodiscard]] virtual auto new_log_file(const std::string &filename, LogFile *&out) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &filename) const -> bool = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &filename, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &filename, std::size_t &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &filename) -> Status = 0;

    virtual auto srand(unsigned seed) -> void = 0;
    [[nodiscard]] virtual auto rand() -> unsigned = 0;

    // Take or upgrade a lock on the given file
    //
    // Return Status::ok() if the lock is granted, Status::busy() if an
    // incompatible lock is already held by a different thread or process,
    // or a Status::io_error() if a system call otherwise fails.
    //
    // Compliant Envs support the following state transitions:
    //
    //     Before -> (Intermediate) -> After
    //    ----------------------------------------
    //     kUnlocked ----------------> kShared
    //     kShared ------------------> kReserved
    //     kShared ------------------> kExclusive
    //     kReserved --> (kPending) -> kExclusive
    //     kPending -----------------> kExclusive
    //
    [[nodiscard]] virtual auto set_lock(File &file, LockMode mode) -> Status = 0;

    // Return the type of lock held on the given file
    [[nodiscard]] virtual auto get_lock(const File &file) const -> LockMode = 0;

    // Release or downgrade a lock on the given file
    //
    // Compliant Envs support the following state transitions (where "> X"
    // represents a lock mode with higher-priority than "X"):
    //
    //     Before -----------> After
    //    ------------------------------
    //     > kUnlocked ------> kUnlocked
    //     > kShared --------> kShared
    //
    // Also note that transitions "Y -> Y", where "Y" <= kShared, are
    // allowed, but are NOOPs.
    //
    [[nodiscard]] virtual auto unlock(File &file, LockMode mode) -> Status = 0;
};

class File
{
public:
    virtual ~File();

    // Attempt to read "size" bytes from the file at the given offset.
    //
    // Reads into "scratch", which must point to at least "size" bytes of available
    // memory. On success, sets "*out" to point to the data that was read, which may
    // be less than what was requested, but only if "out" is not nullptr.
    [[nodiscard]] virtual auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status = 0;

    // Read exactly "size" bytes from the file at the given offset.
    //
    // Return a "not found" status if there is not enough data remaining in the file.
    [[nodiscard]] virtual auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status;

    // Write "in" to the file at the given offset.
    [[nodiscard]] virtual auto write(std::size_t offset, const Slice &in) -> Status = 0;

    // Synchronize with the underlying filesystem.
    [[nodiscard]] virtual auto sync() -> Status = 0;
};

// Construct for logging information about the program.
class LogFile
{
public:
    virtual ~LogFile();

    // Append a message to the log file.
    virtual auto write(const Slice &in) -> void = 0;
};

auto logv(LogFile *file, const char *fmt, ...) -> void;

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &env);
    [[nodiscard]] auto target() -> Env *;
    [[nodiscard]] auto target() const -> const Env *;

    ~EnvWrapper() override;
    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    [[nodiscard]] auto set_lock(File &file, LockMode mode) -> Status override;
    [[nodiscard]] auto get_lock(const File &file) const -> LockMode override;
    [[nodiscard]] auto unlock(File &file, LockMode mode) -> Status override;

private:
    Env *m_target;
};

// Allow composition of open modes with OR
inline constexpr auto operator|(Env::OpenMode lhs, Env::OpenMode rhs) -> Env::OpenMode
{
    return Env::OpenMode {
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}

} // namespace calicodb

#endif // CALICODB_ENV_H

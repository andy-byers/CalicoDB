// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "status.h"

namespace calicodb
{

class File;
class Sink;
class Shm;

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

    // Return a heap-allocated handle to this platform's default Env implementation
    static auto default_env() -> Env *;

    virtual ~Env();

    [[nodiscard]] virtual auto new_sink(const std::string &filename, Sink *&out) -> Status = 0;

    [[nodiscard]] virtual auto open_shm(const std::string &filename, OpenMode mode, Shm *&out) -> Status = 0;
    [[nodiscard]] virtual auto close_shm(Shm *&shm) -> Status = 0;

    [[nodiscard]] virtual auto open_file(const std::string &filename, OpenMode mode, File *&out) -> Status = 0;
    [[nodiscard]] virtual auto close_file(File *&file) -> Status = 0;

    [[nodiscard]] virtual auto resize_file(const std::string &filename, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &filename, std::size_t &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &filename) -> Status = 0;

    /* TODO: remove, just use open("name", Env::kReadOnly, ptr) */ [[nodiscard]] virtual auto file_exists(const std::string &filename) const -> bool = 0;

    virtual auto srand(unsigned seed) -> void = 0;
    [[nodiscard]] virtual auto rand() -> unsigned = 0;
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

    // File locking modes and semantics are from SQLite.
    enum LockMode : int {
        kUnlocked,
        kShared,
        kReserved,
        kPending,
        kExclusive,
    };

    // Take or upgrade a lock on the file
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
    [[nodiscard]] virtual auto lock(LockMode mode) -> Status = 0;

    // Return the type of lock held on the file
    [[nodiscard]] virtual auto lock_mode() const -> LockMode = 0;

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
    virtual auto unlock() -> void = 0;
};

class Sink
{
public:
    virtual ~Sink();
    virtual auto sink(const Slice &in) -> void = 0;
};

auto logv(Sink *sink, const char *fmt, ...) -> void;

class Shm
{
public:
    // Size of a shared memory region, i.e. the number of bytes pointed to by "out"
    // when map() returns successfully.
    static constexpr std::size_t kRegionSize = 1'024 * 32;
    static constexpr std::size_t kLockCount = 8;

    enum LockFlag : int {
        kUnlock = 1,
        kLock = 2,
        kShared = 4,
        kExclusive = 8,
    };

    virtual ~Shm();

    // Map the r'th shared memory region into this process' address space and
    // return a pointer to the first byte
    [[nodiscard]] virtual auto map(std::size_t r, volatile void *&out) -> Status = 0;

    [[nodiscard]] virtual auto lock(std::size_t s, std::size_t n, LockFlag flags) -> Status = 0;
    virtual auto barrier() -> void = 0;
};

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &target);
    [[nodiscard]] auto target() -> Env *;
    [[nodiscard]] auto target() const -> const Env *;

    ~EnvWrapper() override;
    /* TODO: remove */ [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;

    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    [[nodiscard]] auto new_sink(const std::string &filename, Sink *&out) -> Status override;
    [[nodiscard]] auto open_shm(const std::string &filename, OpenMode mode, Shm *&out) -> Status override;
    [[nodiscard]] auto close_shm(Shm *&shm) -> Status override;
    [[nodiscard]] auto open_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto close_file(File *&file) -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

private:
    Env *m_target;
};

class FileWrapper : public File
{
public:
    explicit FileWrapper(File &target);
    ~FileWrapper() override;
    [[nodiscard]] auto target() -> File *;
    [[nodiscard]] auto target() const -> const File *;

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

    [[nodiscard]] auto lock(LockMode mode) -> Status override;
    [[nodiscard]] auto lock_mode() const -> LockMode override;
    auto unlock() -> void override;

private:
    File *m_target;
};

class ShmWrapper : public Shm
{
public:
    explicit ShmWrapper(Shm &target);
    ~ShmWrapper() override;
    [[nodiscard]] auto target() -> Shm *;
    [[nodiscard]] auto target() const -> const Shm *;
    [[nodiscard]] auto map(std::size_t r, volatile void *&out) -> Status override;
    [[nodiscard]] auto lock(std::size_t start, std::size_t n, LockFlag flags) -> Status override;
    auto barrier() -> void override;

private:
    Shm *m_target;
};

// For completeness and in case Sink ever gets additional methods.
class SinkWrapper : public Sink
{
public:
    explicit SinkWrapper(Sink &target);
    ~SinkWrapper() override;
    [[nodiscard]] auto target() -> Sink *;
    [[nodiscard]] auto target() const -> const Sink *;

    auto sink(const Slice &in) -> void override;

private:
    Sink *m_target;
};

// Allow composition of flags with OR
inline constexpr auto operator|(Env::OpenMode lhs, Env::OpenMode rhs) -> Env::OpenMode
{
    return Env::OpenMode{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}
inline constexpr auto operator|(Shm::LockFlag lhs, Shm::LockFlag rhs) -> Shm::LockFlag
{
    return Shm::LockFlag{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}

} // namespace calicodb

#endif // CALICODB_ENV_H

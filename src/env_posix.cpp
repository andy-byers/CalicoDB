// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "env_posix.h"
#include "logging.h"
#include "utils.h"
#include <cstdarg>
#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/stat.h>
#include <unistd.h>

namespace calicodb
{

static constexpr int kFilePermissions = 0644; // -rw-r--r--

enum class PosixCode {
    kOK = 0,
    kNotFound,
    kIOError,
    kBusy,
};

[[nodiscard]] static auto simplify_error(int error) -> PosixCode
{
    CALICODB_EXPECT_NE(error, 0);
    switch (error) {
        case ENOENT:
            return PosixCode::kNotFound;
        case EACCES:
        case EAGAIN:
        case EBUSY:
        case EINTR:
        case ENOLCK:
        case ETIMEDOUT:
            return PosixCode::kBusy;
        default:
            return PosixCode::kIOError;
    }
}

[[nodiscard]] static auto xlate_last_error(PosixCode error) -> Status
{
    switch (error) {
        case PosixCode::kOK:
            return Status::ok();
        case PosixCode::kNotFound:
            return Status::not_found(strerror(errno));
        case PosixCode::kBusy:
            return Status::busy(strerror(errno));
        default:
            return Status::io_error(strerror(errno));
    }
}

[[nodiscard]] static auto xlate_last_error(int error) -> Status
{
    return xlate_last_error(simplify_error(error));
}

[[nodiscard]] static auto posix_file(File &file) -> PosixFile &
{
    return reinterpret_cast<PosixFile &>(file);
}

[[nodiscard]] static auto file_open(const std::string &name, int mode, int permissions, int &out) -> Status
{
    if (const auto fd = open(name.c_str(), mode, permissions); fd >= 0) {
        out = fd;
        return Status::ok();
    }
    out = -1;
    return xlate_last_error(errno);
}

[[nodiscard]] static auto file_read(int file, std::size_t size, char *scratch, Slice *out) -> Status
{
    const auto read_size = size;
    for (std::size_t i = 0; i < read_size; ++i) {
        const auto n = read(file, scratch, size);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return xlate_last_error(errno);
        }
        size -= static_cast<std::size_t>(n);
        if (n == 0 || size == 0) {
            break;
        }
    }
    if (size != 0) {
        std::memset(scratch + read_size - size, 0, size);
    }
    if (out != nullptr) {
        *out = Slice(scratch, read_size - size);
    }
    return Status::ok();
}

[[nodiscard]] static auto file_write(int file, Slice in) -> Status
{
    while (!in.is_empty()) {
        if (const auto n = write(file, in.data(), in.size()); n >= 0) {
            in.advance(static_cast<std::size_t>(n));
        } else if (errno != EINTR) {
            return xlate_last_error(errno);
        }
    }
    return Status::ok();
}

[[nodiscard]] static auto file_sync(int fd) -> Status
{
    if (fsync(fd)) {
        return xlate_last_error(errno);
    }
    return Status::ok();
}

[[nodiscard]] static auto file_seek(int fd, long offset, int whence, std::size_t *out) -> Status
{
    if (const auto position = lseek(fd, offset, whence); position != -1) {
        if (out) {
            *out = static_cast<std::size_t>(position);
        }
        return Status::ok();
    }
    return xlate_last_error(errno);
}

[[nodiscard]] static auto file_remove(const std::string &filename) -> Status
{
    if (unlink(filename.c_str())) {
        return xlate_last_error(errno);
    }
    return Status::ok();
}

[[nodiscard]] static auto file_resize(const std::string &filename, std::size_t size) -> Status
{
    if (truncate(filename.c_str(), static_cast<off_t>(size))) {
        return xlate_last_error(errno);
    }
    return Status::ok();
}

PosixFile::PosixFile(std::string filename, int file)
    : m_filename(std::move(filename)),
      m_file(file)
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixFile::~PosixFile()
{
    close(m_file);
}

auto PosixFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    CALICODB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_read(m_file, size, scratch, out);
}

auto PosixFile::write(std::size_t offset, const Slice &in) -> Status
{
    CALICODB_TRY(file_seek(m_file, static_cast<long>(offset), SEEK_SET, nullptr));
    return file_write(m_file, in);
}

auto PosixFile::sync() -> Status
{
    return file_sync(m_file);
}

PosixLogFile::PosixLogFile(std::string filename, int file)
    : m_buffer(kBufferSize, '\0'),
      m_filename(std::move(filename)),
      m_file(file)
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixLogFile::~PosixLogFile()
{
    close(m_file);
}

auto PosixLogFile::write(const Slice &in) -> void
{
    (void)file_write(m_file, in);
}

EnvPosix::EnvPosix()
    : m_pid(getpid())
{
}

auto EnvPosix::resize_file(const std::string &path, std::size_t size) -> Status
{
    return file_resize(path, size);
}

auto EnvPosix::remove_file(const std::string &path) -> Status
{
    return file_remove(path);
}

auto EnvPosix::file_exists(const std::string &path) const -> bool
{
    return access(path.c_str(), F_OK) == 0;
}

auto EnvPosix::file_size(const std::string &path, std::size_t &out) const -> Status
{
    struct stat st;
    if (stat(path.c_str(), &st)) {
        return xlate_last_error(errno);
    }
    out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto EnvPosix::new_file(const std::string &filename, File *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_CREAT | O_RDWR, kFilePermissions, file));
    out = new PosixFile(filename, file);
    return Status::ok();
}

auto EnvPosix::new_log_file(const std::string &filename, LogFile *&out) -> Status
{
    int file;
    CALICODB_TRY(file_open(filename, O_CREAT | O_WRONLY | O_APPEND, kFilePermissions, file));
    out = new PosixLogFile(filename, file);
    return Status::ok();
}

auto EnvPosix::srand(unsigned seed) -> void
{
    m_rng[0] = 0x330E;
    std::memcpy(&m_rng[1], &seed, sizeof(seed));
}

auto EnvPosix::rand() -> unsigned
{
    return static_cast<unsigned>(nrand48(m_rng));
}

auto split_path(const std::string &filename) -> std::pair<std::string, std::string>
{
    auto *buffer = new char[filename.size() + 1]();

    std::strcpy(buffer, filename.c_str());
    std::string base(basename(buffer));

    std::strcpy(buffer, filename.c_str());
    std::string dir(dirname(buffer));

    delete[] buffer;

    return {dir, base};
}

auto join_paths(const std::string &lhs, const std::string &rhs) -> std::string
{
    return lhs + '/' + rhs;
}

auto cleanup_path(const std::string &filename) -> std::string
{
    const auto [dir, base] = split_path(filename);
    return join_paths(dir, base);
}

//
// Locking sequence. The leftmost column indicates the current lock mode. The
// "#" column indicates the step number, since emulation of these locking modes
// is not exactly 1:1 with the system calls that actually lock files. The rest
// of the columns describe the layout of the lock byte page, starting at byte
// kPendingByte (see below) in the DB file. The "p" means the kPendingByte, the
// "r" means the kReservedByte, and the "s...s" represents kSharedSize bytes
// starting at kSharedFirst. Note that these fields are right next to each other
// in the DB file.
//
//     mode        #  p r s...s
//    --------------------------
//     kUnlocked   1  . . . . .
//     kShared     1  R . . . .
//                 2  R . R...R
//                 3  . . R...R
//     kReserved   1  . . R...R
//                 2  . W R...R
//     kExclusive  1  . W R...R
//                 2  W W R...R
//                 3  W W W...W
//
static constexpr std::size_t kPendingByte = 0x40000000;
static constexpr std::size_t kReservedByte = kPendingByte + 1;
static constexpr std::size_t kSharedFirst = kPendingByte + 2;
static constexpr std::size_t kSharedSize = 510;

enum {kUnlocked = 0};

[[nodiscard]] auto file_lock(int file, const flock &lock) -> int
{
    return fcntl(file, F_SETLK, &lock);
}

auto EnvPosix::lock(File &file, LockMode mode) -> Status
{
    auto &handle = posix_file(file);
    const auto old_mode = handle.m_lock;
    const auto file_num = handle.m_file;
    if (mode <= old_mode) {
        // New lock mode is less restrictive than the one already held.
        return Status::ok();
    }
    CALICODB_EXPECT_TRUE(old_mode != kUnlocked || mode == kShared);
    CALICODB_EXPECT_TRUE(mode != kReserved || old_mode == kShared);
    CALICODB_EXPECT_NE(mode, kPending);

    flock lock = {};
    lock.l_len = 1;
    lock.l_whence = SEEK_SET;
    if (mode == kShared || (mode == kExclusive && old_mode == kReserved)) {
        // Attempt to lock the pending byte.
        lock.l_start = kPendingByte;
        lock.l_type = mode == kShared ? F_RDLCK : F_WRLCK;
        if (const auto rc = file_lock(file_num, lock)) {
            return xlate_last_error(rc);
        } else if (mode == kExclusive) {
            handle.m_lock = kPending;
        }
    }

    auto rc = PosixCode::kOK;
    if (mode == kShared) {
        // Take the shared lock. Type is already set to F_RDLCK in this branch.
        lock.l_start = kSharedFirst;
        lock.l_len = kSharedSize;
        if (file_lock(file_num, lock)) {
            rc = simplify_error(errno);
        }
        
        // Drop the lock on the pending byte.
        lock.l_start = kPendingByte;
        lock.l_len = 1;
        lock.l_type = F_UNLCK;
        if (file_lock(file_num, lock) && rc == PosixCode::kOK) {
            // SQLite says this could happen with a network mount. Such configurations
            // are not yet considered in this design.
            rc = simplify_error(errno);
        }
    } else {
        // The caller is requesting a kReserved or greater. Require that at least a
        // shared lock be held first.
        CALICODB_EXPECT_TRUE(mode == kReserved || mode == kExclusive);
        CALICODB_EXPECT_NE(old_mode, kUnlocked);
        lock.l_type = F_WRLCK;
        if (mode == kReserved) {
            // Lock the reserved byte with a write lock.
            lock.l_start = kReservedByte;
            lock.l_len = 1;
        } else {
            // Lock the whole shared range with a write lock.
            lock.l_start = kSharedFirst;
            lock.l_len = kSharedSize;
        }
        if (file_lock(file_num, lock)) {
            rc = simplify_error(errno);
        }
    }
    if (rc == PosixCode::kOK) {
        handle.m_lock = mode;
    }
    return xlate_last_error(rc);
}

auto EnvPosix::unlock(File &file) -> Status
{
    auto &handle = posix_file(file);
    const auto old_mode = handle.m_lock;
    const auto file_num = handle.m_file;
    flock lock = {};

    if (old_mode == kUnlocked) {
        return Status::ok();
    }
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    if (old_mode > kShared) {
        lock.l_start = kPendingByte;
        // Clear both byte locks (reserved and pending) in the next system call.
        CALICODB_EXPECT_EQ(kPendingByte + 1, kReservedByte);
        lock.l_len = 2;
        if (file_lock(file_num, lock)) {
            return xlate_last_error(errno);
        }
    }
    lock.l_len = 0;
    lock.l_start = 0;
    auto rc = PosixCode::kOK;
    if (file_lock(file_num, lock)) {
        rc = simplify_error(errno);
    }
    handle.m_lock = kUnlocked;
    return xlate_last_error(rc);
}

} // namespace calicodb
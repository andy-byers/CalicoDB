// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "env_posix.h"
#include <fcntl.h>
#include <libgen.h>
#include <list>
#include <mutex>
#include <sys/stat.h>
#include <unistd.h>

namespace calicodb
{

struct FileId final {
    auto operator==(const FileId &rhs) const -> bool
    {
        return device == rhs.device && inode == rhs.inode;
    }

    auto operator!=(const FileId &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    dev_t device = 0;
    U64 inode = 0;
};

struct ShmNode final {
};

// Additional Env::LockMode values.
enum {
    kUnlocked = 0,
};

struct INode final {
    FileId key;
    mutable std::mutex mutex;
    unsigned nshared = 0;
    unsigned nlocks = 0;
    int lock = kUnlocked;

    // List of unused
    std::list<int> pending;
    int refcount = 0;
    ShmNode *shm_node = nullptr;

    // Linked list of inodes. Stored as raw pointers so that inodes can be removed
    // from the list in constant time without having to store std::list iterators
    // in the PosixFile class.
    INode *next = nullptr;
    INode *prev = nullptr;
};

// Per-process singleton for managing filesystem state
struct PosixFs final {
    static PosixFs s_fs;

    ~PosixFs()
    {
        INode *ptr;
        for (const auto *ino = inode; ino; ino = ptr) {
            ptr = ino->next;
            delete ino;
        }
    }

    mutable std::mutex mutex;
    INode *inode = nullptr;
};

PosixFs PosixFs::s_fs;

static constexpr int kFilePermissions = 0644; // -rw-r--r--

enum class PosixCode {
    kOK = 0,
    kNotFound,
    kIOError,
    kBusy,
};

// File locking sequence. The leftmost column indicates the current lock. The
// "#" column indicates the step number, since emulation of these locking modes
// is not exactly 1:1 with the system calls that actually lock files. The rest
// of the columns describe the layout of the lock byte page, starting at byte
// kPendingByte (see below) in the DB file. The "p" means the kPendingByte, the
// "r" means the kReservedByte, and the "s...s" represents kSharedSize bytes
// starting at kSharedFirst. Note that these fields are right next to each other
// in the DB file.
//
//     lock        #  p r s...s
//    --------------------------
//     kUnlocked   1  . . . . .
//     kShared     1  R . . . .
//                 2  R . R...R
//                 3  . . R...R
//     kReserved   1  . W R...R
//     kExclusive  1  W W R...R
//                 2  W W W...W
//
static constexpr std::size_t kPendingByte = 0x40000000;
static constexpr std::size_t kReservedByte = kPendingByte + 1;
static constexpr std::size_t kSharedFirst = kPendingByte + 2;
static constexpr std::size_t kSharedSize = 510;

[[nodiscard]] auto file_lock(int file, const struct flock &lock) -> int
{
    return fcntl(file, F_SETLK, &lock);
}

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

class PosixFile : public File
{
public:
    explicit PosixFile(PosixEnv &env, std::string filename, int file);
    ~PosixFile() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    friend class PosixEnv;
    std::string m_filename;
    INode *m_inode = nullptr;
    PosixEnv *m_env = nullptr;
    int m_file = -1;

    // Lock mode for this particular file descriptor.
    int m_lock = 0;
};

PosixEnv::PosixEnv()
    : m_pid(getpid())
{
}

PosixEnv::~PosixEnv()
{
}

auto PosixEnv::resize_file(const std::string &filename, std::size_t size) -> Status
{
    return file_resize(filename, size);
}

auto PosixEnv::remove_file(const std::string &filename) -> Status
{
    return file_remove(filename);
}

auto PosixEnv::file_exists(const std::string &filename) const -> bool
{
    return access(filename.c_str(), F_OK) == 0;
}

auto PosixEnv::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    struct stat st;
    if (stat(filename.c_str(), &st)) {
        return xlate_last_error(errno);
    }
    out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto PosixEnv::ref_inode(PosixFile &file) -> int
{
    // REQUIRES: s_mutex is locked by the caller
    struct stat statbuf = {};
    FileId key = {};

    const auto &handle = posix_file(file);
    const auto fd = handle.m_file;
    if (fstat(fd, &statbuf)) {
        return -1;
    }

    key.device = statbuf.st_dev;
    key.inode = statbuf.st_ino;

    auto *ino = PosixFs::s_fs.inode;
    while (ino && key != ino->key) {
        ino = ino->next;
    }
    if (ino == nullptr) {
        ino = new INode;

        std::memcpy(&ino->key, &key, sizeof(key));

        ino->refcount = 1;
        ino->next = PosixFs::s_fs.inode;
        ino->prev = nullptr;
        if (PosixFs::s_fs.inode) {
            PosixFs::s_fs.inode->prev = ino;
        }
        PosixFs::s_fs.inode = ino;
    } else {
        ++ino->refcount;
    }
    file.m_inode = ino;
    return 0;
}

auto PosixEnv::unref_inode(PosixFile &file) -> void
{
    auto *&inode = file.m_inode;
    CALICODB_EXPECT_GT(inode->refcount, 0);

    if (--inode->refcount == 0) {
        inode->mutex.lock();
        close_pending_files(file);
        inode->mutex.unlock();

        if (inode->prev) {
            CALICODB_EXPECT_EQ(inode->prev->next, inode);
            inode->prev->next = inode->next;
        } else {
            CALICODB_EXPECT_EQ(PosixFs::s_fs.inode, inode);
            PosixFs::s_fs.inode = inode->next;
        }
        if (inode->next) {
            CALICODB_EXPECT_EQ(inode->next->prev, inode);
            inode->next->prev = inode->prev;
        }
        delete inode;
        inode = nullptr;
    }
}

auto PosixEnv::new_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    const auto is_create = mode & kCreate;
    const auto is_readonly = mode & kReadOnly;
    const auto is_readwrite = mode & kReadWrite;

    // kReadOnly and kReadWrite are mutually exclusive, and files must be created in kReadWrite.
    CALICODB_EXPECT_NE(is_readonly, is_readwrite);
    CALICODB_EXPECT_TRUE(!is_create || is_readwrite);

    const auto flags =
        (is_create ? O_CREAT : 0) |
        (is_readonly ? O_RDONLY : O_RDWR);

    int fd;
    PosixFile *file = nullptr;

    // Open the file. Let the OS choose what file descriptor to use.
    auto s = file_open(filename, flags, kFilePermissions, fd);

    if (s.is_ok()) {
        file = new PosixFile(*this, filename, fd);

        // Search the global inode info list. This requires locking the global mutex.
        std::lock_guard guard(PosixFs::s_fs.mutex);
        if (ref_inode(*file)) {
            delete &file;
            return xlate_last_error(errno);
        }
    }
    out = file;
    return s;
}

auto PosixEnv::new_log_file(const std::string &, LogFile *&out) -> Status
{
    out = nullptr;
    return Status::not_supported("not supported");
}

auto PosixEnv::srand(unsigned seed) -> void
{
    m_rng[0] = 0x330E;
    std::memcpy(&m_rng[1], &seed, sizeof(seed));
}

auto PosixEnv::rand() -> unsigned
{
    return static_cast<unsigned>(nrand48(m_rng));
}

PosixFile::PosixFile(PosixEnv &env, std::string filename, int file)
    : m_filename(std::move(filename)),
      m_env(&env),
      m_file(file)
{
    CALICODB_EXPECT_GE(file, 0);
}

PosixFile::~PosixFile()
{
    CALICODB_EXPECT_TRUE(m_inode);
    (void)m_env->unlock(*this, Env::kUnlocked);

    PosixFs::s_fs.mutex.lock();
    m_inode->mutex.lock();

    if (m_inode->nlocks) {
        PosixEnv::set_pending_file(*this);
    }
    m_inode->mutex.unlock();
    PosixEnv::unref_inode(*this);
    PosixFs::s_fs.mutex.unlock();
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

auto PosixEnv::set_pending_file(PosixFile &file) -> void
{
    auto *inode = file.m_inode;
    inode->pending.emplace_back(file.m_file);
    file.m_file = -1;
}

auto PosixEnv::close_pending_files(PosixFile &file) -> void
{
    // REQUIRES: Mutex "file.m_inode->mutex" is locked by the caller
    auto *inode = file.m_inode;
    for (auto fd : inode->pending) {
        close(fd);
    }
    inode->pending.clear();
}

auto PosixEnv::lock(File &file, LockMode mode) -> Status
{
    auto &handle = posix_file(file);
    const auto old_mode = handle.m_lock;
    const auto file_num = handle.m_file;
    if (mode <= old_mode) {
        // New lock is less restrictive than the one already held.
        return Status::ok();
    }
    // First lock taken on a file descriptor must be kShared. If a reserved lock
    // is being requested, a shared lock must already be held by the caller.
    CALICODB_EXPECT_TRUE(old_mode != kUnlocked || mode == kShared);
    CALICODB_EXPECT_TRUE(mode != kReserved || old_mode == kShared);
    CALICODB_EXPECT_NE(mode, kPending);

    auto *inode = handle.m_inode;
    std::lock_guard guard(inode->mutex);

    if ((old_mode != inode->lock && (inode->lock >= Env::kPending || mode > Env::kShared))) {
        return Status::busy("busy");
    }

    if (mode == Env::kShared && (inode->lock == Env::kShared || inode->lock == Env::kReserved)) {
        // Caller wants a shared lock, and a shared or reserved lock is already
        // held. Grant the request.
        CALICODB_EXPECT_EQ(mode, Env::kShared);
        CALICODB_EXPECT_EQ(handle.m_lock, kUnlocked);
        CALICODB_EXPECT_GT(inode->nshared, 0);
        handle.m_lock = Env::kShared;
        inode->nshared++;
        inode->nlocks++;
        return Status::ok();
    }

    struct flock lock = {};
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
            inode->lock = kPending;
        }
    }

    Status s;
    if (mode == kShared) {
        CALICODB_EXPECT_EQ(inode->nshared, 0);
        CALICODB_EXPECT_EQ(inode->lock, 0);
        // Take the shared lock. Type is already set to F_RDLCK in this branch.
        lock.l_start = kSharedFirst;
        lock.l_len = kSharedSize;
        if (file_lock(file_num, lock)) {
            s = xlate_last_error(errno);
        }

        // Drop the lock on the pending byte.
        lock.l_start = kPendingByte;
        lock.l_len = 1;
        lock.l_type = F_UNLCK;
        if (file_lock(file_num, lock) && s.is_ok()) {
            // SQLite says this could happen with a network mount. Such configurations
            // are not yet considered in this design.
            s = xlate_last_error(errno);
        }
        if (s.is_ok()) {
            inode->nlocks++;
            inode->nshared = 1;
        } else {
            return s;
        }
    } else if (mode == kExclusive && inode->nshared > 1) {
        // Another thread still holds a shared lock, preventing this kExclusive from
        // being taken.
        return Status::busy("busy");
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
            s = xlate_last_error(errno);
        }
    }
    if (s.is_ok()) {
        handle.m_lock = mode;
        inode->lock = mode;
    }
    return s;
}

auto PosixEnv::unlock(File &file, LockMode mode) -> Status
{
    auto &handle = posix_file(file);
    const auto old_mode = handle.m_lock;
    const auto file_num = handle.m_file;
    struct flock lock = {};

    if (old_mode == kUnlocked) {
        return Status::ok();
    }
    auto *inode = handle.m_inode;
    std::lock_guard guard(inode->mutex);
    CALICODB_EXPECT_NE(inode->nshared, 0);

    if (old_mode > kShared) {
        CALICODB_EXPECT_EQ(inode->lock, old_mode);
        if (mode == kShared) {
            lock.l_type = F_RDLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = kSharedFirst;
            lock.l_len = kSharedSize;
            if (file_lock(file_num, lock)) {
                return xlate_last_error(errno);
            }
        }
        lock.l_type = F_UNLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = kPendingByte;
        // Clear both byte locks (reserved and pending) in the next system call.
        CALICODB_EXPECT_EQ(kPendingByte + 1, kReservedByte);
        lock.l_len = 2;
        if (file_lock(file_num, lock)) {
            return xlate_last_error(errno);
        }
        // "file" represents the only file with an exclusive lock, so the inode
        // lock can be downgraded.
        inode->lock = kShared;
    }
    Status s;
    auto rc = PosixCode::kOK;
    if (mode == kUnlocked) {
        --inode->nshared;
        if (inode->nshared == 0) {
            // The last shared lock has been released.
            lock.l_type = F_UNLCK;
            lock.l_whence = SEEK_SET;
            lock.l_len = 0;
            lock.l_start = 0;
            if (file_lock(file_num, lock)) {
                rc = simplify_error(errno);
                // TODO: SQLite seems to set some eFileLocks to NO_LOCK here.
                //       Look into what kinds of errors fcntl(F_SETLK) can
                //       encounter and what happens to the lock state in each case.
            }
            inode->lock = kUnlocked;
            handle.m_lock = kUnlocked;
        }
        if (rc != PosixCode::kOK) {
            s = xlate_last_error(errno);
        }
        CALICODB_EXPECT_GT(inode->nlocks, 0);

        --inode->nlocks;
        if (inode->nlocks == 0) {
            close_pending_files(handle);
        }
    }
    if (rc == PosixCode::kOK) {
        handle.m_lock = mode;
    }
    return s;
}

} // namespace calicodb
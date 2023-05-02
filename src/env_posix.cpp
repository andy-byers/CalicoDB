// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "env_posix.h"
#include <fcntl.h>
#include <libgen.h>
#include <list>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace calicodb
{

class PosixEnv;
class PosixFile;
struct FileId;
struct INode;
struct PosixFs;
struct PosixShm;
struct ShmNode;

static constexpr int kFilePermissions = 0644; // -rw-r--r--

// Constants for SQLite-style shared memory locking
// There are "File::kShmLockCount" lock bytes available. Each byte can be locked
static constexpr std::size_t kShmLock0 = 120;
static constexpr std::size_t kShmDMS = kShmLock0 + File::kShmLockCount;

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
    INode *inode = nullptr;

    mutable std::mutex mutex;

    std::string filename;
    int file = -1;

    bool is_unlocked = false;

    // 32-KB blocks of shared memory.
    std::vector<char *> regions;

    // List of PosixShm objects that reference this ShmNode.
    PosixShm *refs = nullptr;
    std::size_t refcount = 0;

    // Locks held by PosixShm instances in this process. 0 means unlocked, -1 an
    // exclusive lock, and a positive integer N means that N shared locks are
    // held.
    int locks[File::kShmLockCount] = {};

    // Lock the DMS ("dead man switch") byte
    // A reader lock is held on the DMS byte by each shared memory connection.
    // When a connection is dropped, the reader lock is released. A connection
    // knows it is the first connection if it can get a writer lock on the DMS
    // byte.
    [[nodiscard]] auto take_dms_lock() -> int;

    [[nodiscard]] auto map_region(std::size_t r, volatile void *&out) -> int;

#ifdef CALICODB_TEST
    [[nodiscard]] auto check_locks() const -> bool;
#endif // CALICODB_TEST
};

struct INode final {
    FileId key;
    mutable std::mutex mutex;
    unsigned nlocks = 0;
    int lock = kLockUnlocked;

    // List of file descriptors that are waiting to be closed.
    std::list<int> pending;

    // Number of PosixFile instances referencing this inode.
    int refcount = 0;

    // If this inode has had shared memory opened on it, then a pointer to a heap-
    // allocated ShmNode will be stored here. It will be cleaned up when its
    // refcount goes to 0.
    ShmNode *snode = nullptr;

    // Linked list of inodes. Stored as raw pointers so that inodes can be removed
    // from the list in constant time without having to store std::list iterators
    // in the PosixFile class.
    INode *next = nullptr;
    INode *prev = nullptr;
};

// static auto trace_lock(int file, const struct flock &lock) -> void
//{
//     std::fprintf(stderr, "%d ", file);
//     switch (lock.l_type) {
//         case F_RDLCK:
//             std::fputs("RD: ", stderr);
//             break;
//         case F_WRLCK:
//             std::fputs("WR: ", stderr);
//             break;
//         case F_UNLCK:
//             std::fputs("UN: ", stderr);
//             break;
//         default:
//             std::fputs("??: ", stderr);
//     }
//     if (lock.l_start == long(kPendingByte)) {
//         if (lock.l_len == 1) {
//             std::fputs("kPendingByte\n", stderr);
//         } else if (lock.l_len == 2) {
//             std::fputs("kPendingByte && kReservedByte\n", stderr);
//         } else {
//             std::fputs("??\n", stderr);
//         }
//     } else if (lock.l_start == long(kReservedByte)) {
//         if (lock.l_len == 1) {
//             std::fputs("kReserved\n", stderr);
//         } else {
//             std::fputs("??\n", stderr);
//         }
//     } else if (lock.l_start == long(kSharedFirst)) {
//         if (lock.l_len == kSharedSize) {
//             std::fputs("<shared range>\n", stderr);
//         } else {
//             std::fputs("???\n", stderr);
//         }
//     } else {
//         std::fprintf(stderr, " %lld/%lld\n", lock.l_start, lock.l_len);
//     }
// }

[[nodiscard]] static auto posix_file_lock(int file, const struct flock &lock) -> int
{
    return fcntl(file, F_SETLK, &lock);
}

[[nodiscard]] static auto posix_shm_lock(ShmNode &snode, short type, std::size_t offset, std::size_t n) -> int
{
    CALICODB_EXPECT_GE(snode.file, 0);
    CALICODB_EXPECT_TRUE(n == 1 || type != F_RDLCK);
    CALICODB_EXPECT_TRUE(n >= 1 && n <= File::kShmLockCount);

    struct flock lock = {};
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(offset);
    lock.l_len = static_cast<off_t>(n);
    return posix_file_lock(snode.file, lock);
}

[[nodiscard]] static auto posix_error(int error) -> Status
{
    CALICODB_EXPECT_NE(error, 0);
    switch (error) {
        case ENOENT:
            return Status::not_found(std::strerror(error));
        case EACCES:
        case EAGAIN:
        case EBUSY:
        case EINTR:
        case ENOLCK:
        case ETIMEDOUT:
            return Status::busy(std::strerror(error));
        default:
            return Status::io_error(std::strerror(error));
    }
}

[[nodiscard]] static auto posix_busy() -> Status
{
    return Status::busy(strerror(EBUSY));
}

static constexpr std::size_t kOpenCloseTimeout = 100;
[[nodiscard]] static auto posix_open(const std::string &filename, int mode) -> int
{
    for (std::size_t t = 0; t < kOpenCloseTimeout; ++t) {
        const auto fd = open(filename.c_str(), mode | O_CLOEXEC, kFilePermissions);
        if (fd < 0 && errno == EINTR) {
            continue;
        }
        return fd;
    }
    errno = EINTR;
    return -1;
}
[[nodiscard]] static auto posix_close(int fd) -> int
{
    for (std::size_t t = 0; t < kOpenCloseTimeout; ++t) {
        const auto rc = close(fd);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        return rc;
    }
    errno = EINTR;
    return -1;
}

[[nodiscard]] static auto posix_read(int file, std::size_t size, char *scratch, Slice *out) -> int
{
    auto rest = size;
    while (rest > 0) {
        const auto n = read(file, scratch, rest);
        if (n <= 0) {
            if (n == 0) {
                break;
            } else if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        rest -= static_cast<std::size_t>(n);
    }
    std::memset(scratch + size - rest, 0, rest);
    if (out != nullptr) {
        *out = Slice(scratch, size - rest);
    }
    return 0;
}
[[nodiscard]] static auto posix_write(int file, Slice in) -> int
{
    while (!in.is_empty()) {
        const auto n = write(file, in.data(), in.size());
        if (n >= 0) {
            in.advance(static_cast<std::size_t>(n));
        } else if (errno != EINTR) {
            return -1;
        }
    }
    return 0;
}

[[nodiscard]] static auto seek_and_read(int file, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> int
{
    if (const auto rc = lseek(file, static_cast<off_t>(offset), SEEK_SET); rc < 0) {
        return -1;
    }
    return posix_read(file, size, scratch, out);
}
[[nodiscard]] static auto seek_and_write(int file, std::size_t offset, Slice in) -> int
{
    if (const auto rc = lseek(file, static_cast<off_t>(offset), SEEK_SET); rc < 0) {
        return -1;
    }
    return posix_write(file, in);
}

[[nodiscard]] static auto posix_truncate(const std::string &filename, std::size_t size) -> int
{
    for (;;) {
        const auto rc = truncate(filename.c_str(), static_cast<off_t>(size));
        if (rc && errno == EINTR) {
            continue;
        }
        return rc;
    }
}

struct PosixShm {
    [[nodiscard]] auto lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status;

    ShmNode *snode = nullptr;
    PosixShm *next = nullptr;
    U16 reader_mask = 0;
    U16 writer_mask = 0;
};

class PosixFile : public File
{
public:
    explicit PosixFile(std::string filename_, Env::OpenMode mode_, int file_);

    ~PosixFile() override
    {
        (void)close();
    }

    [[nodiscard]] auto close() -> Status;

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override;
    auto file_unlock() -> void override;

    [[nodiscard]] auto shm_map(std::size_t r, volatile void *&out) -> Status override;
    [[nodiscard]] auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override;
    auto shm_unmap(bool unlink) -> void override;
    auto shm_barrier() -> void override;

    const std::string filename;
    INode *inode = nullptr;
    PosixShm *shm = nullptr;
    int file = -1;

    // Lock mode for this particular file descriptor.
    int local_lock = 0;
};

// Per-process singleton for managing filesystem state
struct PosixFs final {
    static PosixFs s_fs;

    explicit PosixFs()
    {
        const auto pgsz = sysconf(_SC_PAGESIZE);
        if (pgsz < 0) {
            page_size = 4'096;
        } else {
            page_size = static_cast<std::size_t>(pgsz);
        }
        if (page_size < File::kShmRegionSize) {
            mmap_scale = 1;
        } else {
            mmap_scale = page_size / File::kShmRegionSize;
        }
    }

    ~PosixFs()
    {
        INode *ptr;
        for (const auto *ino = inode_list; ino; ino = ptr) {
            ptr = ino->next;
            delete ino;
        }
    }

    static auto close_pending_files(INode &inode) -> void
    {
        // REQUIRES: Mutex "inode.mutex" is locked by the caller
        for (auto fd : inode.pending) {
            close(fd);
        }
        inode.pending.clear();
    }

    static int s_x;

    [[nodiscard]] static auto ref_inode(int fd) -> INode *
    {
        // REQUIRES: "s_fs.mutex" is locked by the caller
        s_x++;
        FileId key;
        if (struct stat st = {}; fstat(fd, &st)) {
            return nullptr;
        } else {
            key.device = st.st_dev;
            key.inode = st.st_ino;
        }

        auto *ino = s_fs.inode_list;
        while (ino && key != ino->key) {
            ino = ino->next;
        }
        if (ino == nullptr) {
            ino = new INode;

            ino->key = key;
            ino->refcount = 1;
            ino->next = s_fs.inode_list;
            ino->prev = nullptr;
            if (s_fs.inode_list) {
                s_fs.inode_list->prev = ino;
            }
            s_fs.inode_list = ino;
        } else {
            ++ino->refcount;
        }
        return ino;
    }

    static auto unref_inode(INode *&inode) -> void
    {
        CALICODB_EXPECT_GT(inode->refcount, 0);
        if (--inode->refcount == 0) {
            inode->mutex.lock();
            close_pending_files(*inode);
            inode->mutex.unlock();

            if (inode->prev) {
                CALICODB_EXPECT_EQ(inode->prev->next, inode);
                inode->prev->next = inode->next;
            } else {
                CALICODB_EXPECT_EQ(PosixFs::s_fs.inode_list, inode);
                PosixFs::s_fs.inode_list = inode->next;
            }
            if (inode->next) {
                CALICODB_EXPECT_EQ(inode->next->prev, inode);
                inode->next->prev = inode->prev;
            }
            delete inode;
        }
        inode = nullptr;
    }

    [[nodiscard]] auto ref_snode(PosixFile &file, PosixShm *&out) const -> Status
    {
        auto *inode = file.inode;
        std::unique_lock main_guard(mutex);
        auto *snode = inode->snode;
        if (snode == nullptr) {
            snode = new ShmNode;
            inode->snode = snode;
            snode->inode = inode;
            snode->filename = file.filename + kDefaultShmSuffix;
            snode->file = posix_open(
                snode->filename, O_CREAT | O_NOFOLLOW | O_RDWR);
            if (snode->file < 0) {
                return posix_error(errno);
            }
            // WARNING: If another process unlinks the file after we opened it above, the
            // attempt to take the DMS lock here will fail.
            if (snode->take_dms_lock()) {
                return Status::busy("retry");
            }
        }
        CALICODB_EXPECT_GE(snode->file, 0);
        ++snode->refcount;
        main_guard.unlock();

        std::lock_guard node_guard(snode->mutex);
        out = new PosixShm();
        out->snode = snode;
        out->next = snode->refs;
        snode->refs = out;
        return Status::ok();
    }

    auto unref_snode(PosixShm &shm, bool unlink_if_last) const -> void
    {
        auto *snode = shm.snode;
        auto *inode = snode->inode;
        {
            std::lock_guard guard(snode->mutex);
            auto **pp = &snode->refs;
            for (; *pp != &shm; pp = &(*pp)->next) {
            }
            // Remove "shm" from the list.
            *pp = shm.next;
        }

        // Global mutex must be locked when creating or destroying shm nodes.
        std::lock_guard guard(mutex);
        CALICODB_EXPECT_GT(snode->refcount, 0);

        if (--snode->refcount == 0) {
            const auto interval = mmap_scale;
            for (std::size_t i = 0; i < snode->regions.size(); i += interval) {
                munmap(snode->regions[i], File::kShmRegionSize);
            }
            if (unlink_if_last) {
                // Take a write lock on the DMS byte to make sure no other processes are
                // using this shm file.
                if (0 == posix_shm_lock(*snode, F_WRLCK, kShmDMS, 1)) {
                    // This should drop the lock we just took.
                    unlink(snode->filename.c_str());
                }
            }
            (void)posix_close(snode->file);
            delete snode;
            inode->snode = nullptr;
        }
    }

    // Linked list of inodes, protected by a mutex.
    mutable std::mutex mutex;
    INode *inode_list = nullptr;

    // OS page size for mmap().
    std::size_t page_size = 0;

    // The OS page size may be greater than the shared memory region
    // size (File::kShmRegionSize). If so, then mmap() must allocate this
    // number of regions each time it is called.
    std::size_t mmap_scale = 0;
};

PosixFs PosixFs::s_fs;
int PosixFs::s_x = 0;

auto PosixEnv::resize_file(const std::string &filename, std::size_t size) -> Status
{
    if (posix_truncate(filename, size)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixEnv::remove_file(const std::string &filename) -> Status
{
    if (unlink(filename.c_str())) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixEnv::file_exists(const std::string &filename) const -> bool
{
    return access(filename.c_str(), F_OK) == 0;
}

auto PosixEnv::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    struct stat st = {};
    if (stat(filename.c_str(), &st)) {
        return posix_error(errno);
    }
    out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto PosixEnv::new_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    const auto is_create = mode & kCreate;
    const auto is_readonly = mode & kReadOnly;

    const auto flags =
        (is_create ? O_CREAT : 0) |
        (is_readonly ? O_RDONLY : O_RDWR);

    // Open the file. Let the OS choose what file descriptor to use.
    const auto fd = posix_open(filename, flags);
    if (fd < 0) {
        return posix_error(errno);
    }
    auto *file = new PosixFile(filename, mode, fd);
    // Search the global inode info list. This requires locking the global mutex.
    std::lock_guard guard(PosixFs::s_fs.mutex);
    auto *inode = PosixFs::ref_inode(fd);
    if (inode == nullptr) {
        delete file;
        return posix_error(errno);
    }
    file->inode = inode;
    out = file;
    return Status::ok();
}

auto PosixEnv::new_sink(const std::string &filename, Sink *&out) -> Status
{
    out = nullptr; // TODO
    return Status::not_supported(filename + " cannot be opened... PosixSink is not yet implemented");
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

auto PosixEnv::sleep(unsigned micros) -> void
{
    static constexpr unsigned kMicrosPerSecond = 1'000'000;
    if (micros >= kMicrosPerSecond) {
        ::sleep(micros / kMicrosPerSecond);
    }
    if (micros % kMicrosPerSecond) {
        ::usleep(micros % kMicrosPerSecond);
    }
}

PosixFile::PosixFile(std::string filename_, Env::OpenMode, int file_)
    : filename(std::move(filename_)),
      file(file_)
{
    CALICODB_EXPECT_GE(file, 0);
}

auto PosixFile::close() -> Status
{
    auto fd = file;
    file = -1;

    if (fd < 0) {
        // Already closed.
        return Status::ok();
    }
    CALICODB_EXPECT_TRUE(inode);
    CALICODB_EXPECT_FALSE(shm);
    file_unlock();

    PosixFs::s_fs.mutex.lock();
    inode->mutex.lock();

    if (inode->nlocks) {
        // Some other thread in this process has a lock on this file from
        // a different file descriptor. Closing this file descriptor will
        // cause other threads to lose their locks. Defer close() until
        // the other locks have been released.
        inode->pending.emplace_back(fd);
        fd = -1;
    }
    inode->mutex.unlock();
    PosixFs::unref_inode(inode);
    PosixFs::s_fs.mutex.unlock();

    if (fd >= 0 && posix_close(fd)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    if (seek_and_read(file, offset, size, scratch, out)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::write(std::size_t offset, const Slice &in) -> Status
{
    if (seek_and_write(file, offset, in)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::sync() -> Status
{
    if (fsync(file)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::shm_unmap(bool unlink) -> void
{
    if (shm) {
        PosixFs::s_fs.unref_snode(*shm, unlink);

        delete shm;
        shm = nullptr;
    }
}

auto PosixFile::shm_map(std::size_t r, volatile void *&out) -> Status
{
    if (shm == nullptr) {
        CALICODB_TRY(PosixFs::s_fs.ref_snode(*this, shm));
    }
    CALICODB_EXPECT_TRUE(shm);
    CALICODB_EXPECT_TRUE(shm->snode);
    if (shm->snode->map_region(r, out)) {
        if (errno == EAGAIN) {
            return Status::busy("retry");
        }
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status
{
    if (shm) {
        return shm->lock(r, n, flags);
    }
    return Status::io_error("unmapped");
}

auto PosixFile::shm_barrier() -> void
{
    __sync_synchronize();
}

auto PosixShm::lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status
{
    CALICODB_EXPECT_LE(r + n, File::kShmLockCount);
    CALICODB_EXPECT_GT(n, 0);
    CALICODB_EXPECT_TRUE(
        flags == (kShmLock | kShmReader) ||
        flags == (kShmLock | kShmWriter) ||
        flags == (kShmUnlock | kShmReader) ||
        flags == (kShmUnlock | kShmWriter));
    CALICODB_EXPECT_TRUE(n == 1 || (flags & kShmWriter));

    auto *state = snode->locks;
    const auto mask = static_cast<U16>((1 << (r + n)) - (1 << r));
    CALICODB_EXPECT_TRUE(n > 1 || mask == (1 << r));
    std::lock_guard guard(snode->mutex);
    CALICODB_EXPECT_TRUE(snode->check_locks());

    if (flags & kShmUnlock) {
        if ((reader_mask | writer_mask) & mask) {
            auto unlock = true;
            // Determine whether another thread in this process has a shared lock. Don't
            // worry about exclusive locks here: if there is one, then it must be ours,
            // given that this thread is following the locking protocol.
            for (auto i = r; i < r + n; ++i) {
                // shared_bit is true if this PosixShm has a shared lock on bit i, false
                // otherwise. If shared_bit is false, then this thread must have an
                // exclusive lock on bit i, otherwise we are trying to unlock bytes that
                // are not locked.
                const bool shared_bit = reader_mask & (1 << i);
                if (state[i] > shared_bit) {
                    unlock = false;
                }
            }

            if (unlock) {
                if (posix_shm_lock(*snode, F_UNLCK, r + kShmLock0, n)) {
                    return posix_error(errno);
                }
                std::memset(&state[r], 0, sizeof(int) * n);
            } else {
                CALICODB_EXPECT_TRUE(reader_mask & (1 << r));
                CALICODB_EXPECT_TRUE(n == 1 && state[r] > 1);
                --state[r];
            }
            writer_mask &= ~mask;
            reader_mask &= ~mask;
        }
    } else if (flags & kShmReader) {
        CALICODB_EXPECT_EQ(0, writer_mask & (1 << r));
        CALICODB_EXPECT_EQ(1, n);
        if ((reader_mask & mask) == 0) {
            if (state[r] < 0) {
                return posix_busy();
            } else if (state[r] == 0) {
                if (posix_shm_lock(*snode, F_RDLCK, r + kShmLock0, n)) {
                    return posix_error(errno);
                }
            }
            reader_mask |= mask;
            state[r]++;
        }
    } else {
        // Take writer locks on bytes r through r + n - 1, inclusive. There
        // should not be a reader lock on any of these bytes from this thread
        // (otherwise, this thread forgot to release its reader lock on one of
        // these bytes before attempting a writer lock).
        CALICODB_EXPECT_FALSE(reader_mask & mask);
        for (std::size_t i = r; i < r + n; ++i) {
            if ((writer_mask & (1 << i)) == 0 && state[i]) {
                // Some other thread in this process has a lock.
                return posix_busy();
            }
        }

        if (posix_shm_lock(*snode, F_WRLCK, r + kShmLock0, n)) {
            // Some thread in another process has a lock.
            return posix_error(errno);
        }
        CALICODB_EXPECT_FALSE(reader_mask & mask);
        std::fill(state + r, state + r + n, -1);
        writer_mask |= mask;
    }
    CALICODB_EXPECT_TRUE(snode->check_locks());
    return Status::ok();
}

auto ShmNode::take_dms_lock() -> int
{
    struct flock lock = {};
    lock.l_whence = SEEK_SET;
    lock.l_start = kShmDMS;
    lock.l_len = 1;
    lock.l_type = F_WRLCK;

    int rc = 0;
    if (fcntl(file, F_GETLK, &lock)) {
        rc = -1;
    } else if (lock.l_type == F_UNLCK) {
        // The DMS byte is unlocked, meaning this must be the first connection.
        rc = posix_shm_lock(*this, F_WRLCK, kShmDMS, 1);
        if (rc == 0) {
            rc = posix_truncate(filename, 0);
        }
    } else if (lock.l_type == F_WRLCK) {
        // A different connection was the first connection, and is in the
        // process of truncating the file.
        errno = EAGAIN;
        rc = -1;
    }
    if (rc == 0) {
        // Take a read lock on the DMS byte (maybe downgrading from a write
        // lock if this was the first connection). Every process using this
        // shared memory should have a lock on this byte.
        rc = posix_shm_lock(*this, F_RDLCK, kShmDMS, 1);
    }
    return rc;
}

auto ShmNode::map_region(std::size_t r, volatile void *&out) -> int
{
    std::lock_guard guard(mutex);
    if (is_unlocked) {
        if (auto rc = take_dms_lock()) {
            errno = EAGAIN;
            return rc;
        }
        is_unlocked = false;
    }
    // Determine the file size (in shared memory regions) needed to satisfy the
    // request for region "r".
    const auto mmap_scale = PosixFs::s_fs.mmap_scale;
    const auto request = (r + mmap_scale) / mmap_scale * mmap_scale;

    if (regions.size() < request) {
        std::size_t file_size;
        if (struct stat st = {}; fstat(file, &st)) {
            return -1;
        } else {
            file_size = static_cast<std::size_t>(st.st_size);
        }
        if (file_size < request * File::kShmRegionSize) {
            // Write a '\0' to the end of the highest-addressed region to extend the
            // file. SQLite writes a byte to the end of each OS page, causing the pages
            // to be allocated immediately (to reduce the chance of a later SIGBUS).
            // This should be good enough for now.
            if (seek_and_write(file, request * File::kShmRegionSize - 1, Slice("", 1))) {
                return -1;
            }
        }

        while (regions.size() < request) {
            // Map "mmap_scale" shared memory regions into this address space.
            auto *p = mmap(
                nullptr, File::kShmRegionSize * mmap_scale,
                PROT_READ | PROT_WRITE,
                MAP_SHARED, file,
                static_cast<ssize_t>(File::kShmRegionSize * regions.size()));
            if (p == MAP_FAILED) {
                return -1;
            }
            // Store a pointer to the start of each memory region.
            for (std::size_t i = 0; i < mmap_scale; ++i) {
                regions.emplace_back(reinterpret_cast<char *>(p) + File::kShmRegionSize * i);
            }
        }
    }
    CALICODB_EXPECT_LT(r, regions.size());
    out = regions[r];
    return 0;
}

#ifdef CALICODB_TEST
auto ShmNode::check_locks() const -> bool
{
    // REQUIRES: "snode->mutex" is locked
    int check[File::kShmLockCount] = {};

    for (auto *p = refs; p; p = p->next) {
        for (std::size_t i = 0; i < File::kShmLockCount; ++i) {
            if (p->writer_mask & (1 << i)) {
                CALICODB_EXPECT_FALSE(check[i]);
                check[i] = -1;
            } else if (p->reader_mask & (1 << i)) {
                CALICODB_EXPECT_GE(check[i], 0);
                ++check[i];
            }
        }
    }

    const auto result = std::memcmp(locks, check, sizeof(check));
    CALICODB_EXPECT_EQ(0, result);
    return result == 0;
}
#endif // CALICODB_TEST

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

auto PosixFile::file_lock(FileLockMode mode) -> Status
{
    if (mode <= local_lock) {
        return Status::ok();
    }
    // First lock taken on a file must be kShared.
    CALICODB_EXPECT_TRUE(local_lock != kLockUnlocked || mode == kLockShared);

    std::lock_guard guard(inode->mutex);
    if ((local_lock != inode->lock && (inode->lock == kLockExclusive || mode == kLockExclusive))) {
        // Some other thread in this process has an incompatible lock.
        return posix_busy();
    }

    if (mode == kLockShared && inode->lock == kLockShared) {
        // Caller wants a shared lock, and a shared lock is already held by another thread.
        // Grant the mode. This block is just to avoid actually calling out to fcntl(),
        // since we already know this lock is compatible.
        CALICODB_EXPECT_EQ(local_lock, kLockUnlocked);
        CALICODB_EXPECT_GT(inode->nlocks, 0);
        local_lock = kLockShared;
        inode->nlocks++;
        return Status::ok();
    }
    struct flock lock = {};
    lock.l_len = 0;
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;

    Status s;
    if (mode == kLockShared) {
        // Requesting a shared lock, but didn't hit the above block. This means
        // no other thread in this process holds a lock, so we need to check to
        // see if another process holds one that is incompatible.
        CALICODB_EXPECT_EQ(inode->lock, kLockUnlocked);
        CALICODB_EXPECT_EQ(inode->nlocks, 0);
        lock.l_type = F_RDLCK;
        if (posix_file_lock(file, lock)) {
            s = posix_error(errno);
        } else {
            inode->nlocks = 1;
        }
    } else if (mode == kLockExclusive && inode->nlocks > 1) {
        // Another thread in this process still holds a shared lock, preventing
        // this kExclusive from being taken. Note that this thread should already
        // have a shared lock (guarded for by an assert).
        s = posix_busy();
    } else {
        // The caller is requesting an exclusive lock, and no other thread in
        // this process already holds a lock.
        CALICODB_EXPECT_EQ(mode, kLockExclusive);
        CALICODB_EXPECT_NE(local_lock, kLockUnlocked);
        CALICODB_EXPECT_EQ(inode->nlocks, 1);
        lock.l_type = F_WRLCK;
        if (posix_file_lock(file, lock)) {
            s = posix_error(errno);
        }
    }
    if (s.is_ok()) {
        local_lock = mode;
        inode->lock = mode;
    }
    return s;
}

auto PosixFile::file_unlock() -> void
{
    if (local_lock == kLockUnlocked) {
        return;
    }

    struct flock lock = {};
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;

    std::lock_guard guard(inode->mutex);
    CALICODB_EXPECT_TRUE(inode->lock == kLockShared || inode->nlocks == 1);
    CALICODB_EXPECT_GT(inode->nlocks, 0);

    if (--inode->nlocks == 0) {
        (void)posix_file_lock(file, lock);
        PosixFs::close_pending_files(*inode);
        inode->lock = kLockUnlocked;
    }
    local_lock = kLockUnlocked;
}

} // namespace calicodb
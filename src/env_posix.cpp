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

namespace calicodb
{

class PosixEnv;
class PosixFile;
class PosixShm;
struct FileId;
struct INode;
struct PosixFs;
struct ShmNode;

static constexpr int kFilePermissions = 0644; // -rw-r--r--

// File locking sequence. The leftmost column indicates the current lock. The
// "#" column indicates the step number, since emulation of these locking modes
// is not exactly 1:1 with the system calls that actually lock files. The rest
// of the columns describe the layout of the lock byte page, starting at byte
// kPendingByte (see below) in the DB file. The "p" means the kPendingByte, the
// "r" means the kReservedByte, and the "s...s" represents kSharedSize bytes
// starting at kSharedFirst. Note that these fields are right next to each other
// in the DB file.
//
//     Lock        #  p r s...s
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

// Constants for SQLite-style shared memory locking.
static constexpr std::size_t kShmLock0 = 120;
static constexpr std::size_t kShmDMS = kShmLock0 + Shm::kLockCount;

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

    bool is_readonly = false;
    bool is_unlocked = false;

    // 32-KB blocks of shared memory.
    std::vector<char *> regions;

    // List of PosixShm objects that reference this ShmNode.
    PosixShm *refs = nullptr;
    std::size_t refcount = 0;

    // Locks held by PosixShm instances in this process. 0 means unlocked, -1 an
    // exclusive lock, and a positive integer N means that N shared locks are
    // held.
    int locks[Shm::kLockCount] = {};

    static constexpr int kReadonlyTriedReset = -2;
    [[nodiscard]] auto take_dms_lock() -> int;
    [[nodiscard]] auto map_region(std::size_t r, volatile void *&out) -> int;
    [[nodiscard]] auto destroy() && -> Status;

#ifdef CALICODB_TEST
    [[nodiscard]] auto check_locks() const -> bool;
#endif // CALICODB_TEST
};

struct INode final {
    FileId key;
    mutable std::mutex mutex;
    unsigned nshared = 0;
    unsigned nlocks = 0;
    int lock = File::kUnlocked;

    // List of file descriptors that are waiting to be closed. If a file has an
    // advisory lock on it, it cannot be closed.
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

[[nodiscard]] static auto file_lock(int file, const struct flock &lock) -> int
{
    return fcntl(file, F_SETLK, &lock);
}

[[nodiscard]] static auto shm_file_lock(ShmNode &snode, short type, std::size_t offset, std::size_t n) -> int
{
    CALICODB_EXPECT_GE(snode.file, 0);
    CALICODB_EXPECT_TRUE(n == 1 || type != F_RDLCK);
    CALICODB_EXPECT_TRUE(n >= 1 && n <= Shm::kLockCount);

    struct flock lock = {}; // TODO: PID?
    lock.l_type = type;
    lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(offset);
    lock.l_len = static_cast<off_t>(n);
    return file_lock(snode.file, lock);
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

// Cast to POSIX-specific derived classes at the interface boundary
[[nodiscard]] static auto posix_file(File &file) -> PosixFile &
{
    return reinterpret_cast<PosixFile &>(file);
}
[[nodiscard]] static auto posix_shm(Shm &shm) -> PosixShm &
{
    return reinterpret_cast<PosixShm &>(shm);
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
        rest -= n;
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

class PosixFile : public File
{
public:
    explicit PosixFile(PosixEnv &env, std::string filename, Env::OpenMode mode, int file);

    ~PosixFile() override
    {
        (void)close();
    }

    [[nodiscard]] auto close() -> Status;

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto lock(LockMode mode) -> Status override;
    [[nodiscard]] auto lock_mode() const -> LockMode override;
    auto unlock() -> void override;

    const std::string filename;
    INode *inode = nullptr;
    PosixEnv *env = nullptr;
    Env::OpenMode openmode;
    int file = -1;

    // Lock mode for this particular file descriptor.
    int lockmode = 0;
};

class PosixShm : public Shm
{
public:
    explicit PosixShm();
    ~PosixShm() override;

    [[nodiscard]] auto map(std::size_t r, volatile void *&out) -> Status override;
    [[nodiscard]] auto lock(std::size_t r, std::size_t n, LockFlag flags) -> Status override;
    auto barrier() -> void override;

    PosixFile *file = nullptr;
    ShmNode *snode = nullptr;
    PosixShm *next = nullptr;
    U16 shared_mask = 0;
    U16 exclusive_mask = 0;
};

// Per-process singleton for managing filesystem state
struct PosixFs final {
    static PosixFs s_fs;

    explicit PosixFs()
        : page_size(sysconf(_SC_PAGESIZE))
    {
        if (page_size < Shm::kRegionSize) {
            mmap_scale = 1;
        } else {
            mmap_scale = page_size / Shm::kRegionSize;
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

    [[nodiscard]] static auto ref_inode(int fd) -> INode *
    {
        // REQUIRES: "s_fs.mutex" is locked by the caller

        FileId key;
        if (struct stat st; fstat(fd, &st)) {
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

    auto ref_shm(PosixFile &file) -> PosixShm *
    {
        auto *inode = file.inode;
        std::unique_lock main_guard(mutex);
        auto *snode = inode->snode;
        if (snode == nullptr) {
            snode = new ShmNode;
            inode->snode = snode;
            snode->inode = inode;
            snode->filename = file.filename;
            snode->is_readonly = (file.openmode & Env::kReadWrite) == 0;
            const auto rw_mode = snode->is_readonly ? O_RDONLY : O_RDWR;
            snode->file = posix_open(file.filename, O_CREAT | O_NOFOLLOW | rw_mode);
            if (snode->file < 0) {
                return nullptr;
            }
            if (snode->take_dms_lock()) {
                // TODO: Failure to truncate the file b/c readonly.
                return nullptr;
            }
        }
        CALICODB_EXPECT_GE(snode->file, 0);
        ++snode->refcount;
        main_guard.unlock();
        std::lock_guard node_guard(snode->mutex);
        auto *shm = new PosixShm();
        shm->file = &file;
        shm->snode = snode;
        shm->next = snode->refs;
        snode->refs = shm;
        return shm;
    }

    [[nodiscard]] auto unref_shm(PosixShm *&shm) -> Status
    {
        auto *snode = shm->snode;
        auto *inode = snode->inode;
        {
            std::lock_guard guard(snode->mutex);
            auto **pp = &snode->refs;
            for (; *pp != shm; pp = &(*pp)->next) {
            }
            // Remove "shm" from the list.
            *pp = shm->next;
        }

        std::lock_guard guard(PosixFs::s_fs.mutex);
        CALICODB_EXPECT_GT(snode->refcount, 0);

        Status s;
        if (--snode->refcount == 0) {
            s = std::move(*inode->snode).destroy();
            delete inode->snode;
            inode->snode = nullptr;
        }
        delete shm;
        shm = nullptr;
        return s;
    }

    // Linked list of inodes, protected by a mutex.
    mutable std::mutex mutex;
    INode *inode_list = nullptr;

    // OS page size for mmap().
    std::size_t page_size = 0;

    // The OS page size may be greater than the shared memory region
    // size (Shm::kRegionSize). If so, then mmap() must allocate this
    // number of regions each time it is called.
    std::size_t mmap_scale = 0;
};

PosixFs PosixFs::s_fs;

PosixEnv::PosixEnv()
    : m_pid(getpid())
{
}

PosixEnv::~PosixEnv()
{
}

auto PosixEnv::open_shm(const std::string &filename, OpenMode mode, Shm *&out) -> Status
{
    File *file;
    CALICODB_TRY(open_file(filename, mode, file));
    out = PosixFs::s_fs.ref_shm(posix_file(*file));
    return Status::ok();
}

auto PosixEnv::close_shm(Shm *&shm) -> Status
{
    if (shm == nullptr) {
        return Status::ok();
    }
    auto *ptr = &posix_shm(*shm);
    auto *file = ptr->file;
    shm = nullptr;

    auto s = PosixFs::s_fs.unref_shm(ptr);
    auto t = file->close();
    return s.is_ok() ? t : s;
}

auto PosixEnv::close_file(File *&file) -> Status
{
    Status s;
    if (file) {
        s = posix_file(*file).close();
    }
    delete file;
    file = nullptr;
    return s;
}

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
    struct stat st;
    if (stat(filename.c_str(), &st)) {
        return posix_error(errno);
    }
    out = static_cast<std::size_t>(st.st_size);
    return Status::ok();
}

auto PosixEnv::open_file(const std::string &filename, OpenMode mode, File *&out) -> Status
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

    // Open the file. Let the OS choose what file descriptor to use.
    const auto fd = posix_open(filename, flags);
    if (fd < 0) {
        return posix_error(errno);
    }
    auto *file = new PosixFile(*this, filename, mode, fd);
    // Search the global inode info list. This requires locking the global mutex.
    std::lock_guard guard(PosixFs::s_fs.mutex);
    auto *inode = PosixFs::ref_inode(fd);
    if (inode == nullptr) {
        delete &file;
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

PosixFile::PosixFile(PosixEnv &env, std::string filename, Env::OpenMode mode, int file)
    : filename(std::move(filename)),
      env(&env),
      openmode(mode),
      file(file)
{
    (void)env;
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
    unlock();

    PosixFs::s_fs.mutex.lock();
    inode->mutex.lock();

    if (inode->nlocks) {
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

PosixShm::PosixShm()
{
}

PosixShm::~PosixShm()
{
}

auto PosixShm::map(std::size_t r, volatile void *&out) -> Status
{
    if (snode->map_region(r, out)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixShm::lock(std::size_t r, std::size_t n, LockFlag flags) -> Status
{
    CALICODB_EXPECT_LE(r + n, Shm::kLockCount);
    CALICODB_EXPECT_GT(n, 0);
    CALICODB_EXPECT_TRUE(
        flags == (Shm::kLock | Shm::kShared) ||
        flags == (Shm::kLock | Shm::kExclusive) ||
        flags == (Shm::kUnlock | Shm::kShared) ||
        flags == (Shm::kUnlock | Shm::kExclusive));
    CALICODB_EXPECT_TRUE(n == 1 || (flags & Shm::kExclusive));

    auto *state = snode->locks;
    const auto mask = (1 << (r + n)) - (1 << r);
    CALICODB_EXPECT_TRUE(n > 1 || mask == (1 << r));
    std::lock_guard guard(snode->mutex);
    CALICODB_EXPECT_TRUE(snode->check_locks());

    if (flags & Shm::kUnlock) {
        if ((shared_mask | exclusive_mask) & mask) {
            auto unlock = true;
            // Determine whether another thread in this process has a shared lock. Don't
            // worry about exclusive locks here: if there is one, then it must be ours,
            // given that this thread is following the locking protocol.
            for (auto i = r; i < r + n; ++i) {
                // shared_bit is true if this PosixShm has a shared lock on bit i, false
                // otherwise. If shared_bit is false, then this thread must have an
                // exclusive lock on bit i, otherwise we are trying to unlock bytes that
                // are not locked.
                const bool shared_bit = shared_mask & (1 << i);
                if (state[i] > shared_bit) {
                    unlock = false;
                }
            }

            if (unlock) {
                if (shm_file_lock(*snode, F_UNLCK, r + kShmLock0, n)) {
                    return posix_error(errno);
                }
                std::memset(&state[r], 0, sizeof(int) * n);
            } else {
                CALICODB_EXPECT_TRUE(shared_mask & (1 << r));
                CALICODB_EXPECT_TRUE(n == 1 && state[r] > 1);
                --state[r];
            }
            exclusive_mask &= ~mask;
            shared_mask &= ~mask;
        }
    } else if (flags & Shm::kShared) {
        CALICODB_EXPECT_EQ(0, exclusive_mask & (1 << r));
        CALICODB_EXPECT_EQ(1, n);
        if ((shared_mask & mask) == 0) {
            if (state[r] < 0) {
                return posix_busy();
            } else if (state[r] == 0) {
                if (shm_file_lock(*snode, F_RDLCK, r + kShmLock0, n)) {
                    return posix_error(errno);
                }
            }
            shared_mask |= mask;
            state[r]++;
        }
    } else {
        // Take exclusive locks on bytes s through s + n - 1, inclusive.
        CALICODB_EXPECT_FALSE(shared_mask & mask);
        for (std::size_t i = r; i < r + n; ++i) {
            if ((exclusive_mask & (1 << i)) == 0 && state[i]) {
                // Some other thread in this process has a lock.
                return posix_busy();
            }
        }

        if (shm_file_lock(*snode, F_WRLCK, r + kShmLock0, n)) {
            // Some other thread in another process has a lock.
            return posix_error(errno);
        }
        CALICODB_EXPECT_FALSE(shared_mask & mask);
        std::fill(state + r, state + r + n, -1);
        exclusive_mask |= mask;
    }
    CALICODB_EXPECT_TRUE(snode->check_locks());
    return Status::ok();
}

auto PosixShm::barrier() -> void
{
    // TODO: This is what SQLite does, but they also provide the compile option/macro SQLITE_MEMORY_BARRIER
    //       which allows one to perform some custom action when barrier() is called.
#if defined(__GNUC__) && GCC_VERSION >= 4001000
    __sync_synchronize();
#endif
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
        if (is_readonly) {
            is_unlocked = true;
            rc = kReadonlyTriedReset;
        } else {
            rc = shm_file_lock(*this, F_WRLCK, kShmDMS, 1);
            if (rc == 0) {
                rc = posix_truncate(filename, 0);
            }
        }
    } else if (lock.l_type == F_WRLCK) {
        errno = EAGAIN;
        rc = -1;
    }
    if (rc == 0) {
        // Take a read lock on the DMS byte (maybe downgrading from a write
        // lock if this was the first connection). Every process using this
        // shared memory should have a lock on this byte.
        rc = shm_file_lock(*this, F_RDLCK, kShmDMS, 1);
    }
    return rc;
}

auto ShmNode::map_region(std::size_t r, volatile void *&out) -> int
{
    std::lock_guard guard(mutex);
    if (is_unlocked) {
        if (auto rc = take_dms_lock()) {
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
        if (struct stat st; fstat(file, &st)) {
            return -1;
        } else {
            file_size = st.st_size;
        }
        if (file_size < request * Shm::kRegionSize) {
            // Write a '\0' to the end of the highest-addressed region to extend the
            // file. SQLite writes a byte to the end of each OS page, causing the pages
            // to be allocated immediately (to reduce the chance of a later SIGBUS).
            // This should be good enough for now.
            if (seek_and_write(file, request * Shm::kRegionSize - 1, Slice("", 1))) {
                return -1;
            }
        }

        while (regions.size() < request) {
            // Map "mmap_scale" shared memory regions into this address space.
            auto *p = mmap(
                nullptr, Shm::kRegionSize * mmap_scale,
                (is_readonly ? 0 : PROT_WRITE) | PROT_READ,
                MAP_SHARED, file,
                static_cast<ssize_t>(Shm::kRegionSize * regions.size()));
            if (p == MAP_FAILED) {
                return -1;
            }
            // Store a pointer to the start of each memory region.
            for (std::size_t i = 0; i < mmap_scale; ++i) {
                regions.emplace_back(reinterpret_cast<char *>(p) + Shm::kRegionSize * i);
            }
        }
    }
    CALICODB_EXPECT_LT(r, regions.size());
    out = regions[r];
    return 0;
}

auto ShmNode::destroy() && -> Status
{
    // REQUIRES: PosixFs::s_fs.mutex is locked
    CALICODB_EXPECT_EQ(refcount, 0);
    CALICODB_EXPECT_GE(file, 0);
    const auto interval = PosixFs::s_fs.mmap_scale;
    for (std::size_t i = 0; i < regions.size(); i += interval) {
        munmap(regions[i], Shm::kRegionSize);
    }
    inode->snode = nullptr;
    if (posix_close(file)) {
        return posix_error(errno);
    }
    return Status::ok();
}

#ifdef CALICODB_TEST
auto ShmNode::check_locks() const -> bool
{
    // REQUIRES: "snode->mutex" is locked
    int check[Shm::kLockCount] = {};

    for (auto *p = refs; p; p = p->next) {
        for (std::size_t i = 0; i < Shm::kLockCount; ++i) {
            if (p->exclusive_mask & (1 << i)) {
                CALICODB_EXPECT_FALSE(check[i]);
                check[i] = -1;
            } else if (p->shared_mask & (1 << i)) {
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

auto PosixFile::lock_mode() const -> LockMode
{
    return LockMode{lockmode};
}

auto PosixFile::lock(LockMode mode) -> Status
{
    if (mode <= lockmode) {
        // New lock is less restrictive than the one already held.
        return Status::ok();
    }
    // First lock taken on a file descriptor must be kShared. If a reserved lock
    // is being requested, a shared lock must already be held by the caller.
    CALICODB_EXPECT_TRUE(lockmode != kUnlocked || mode == kShared);
    CALICODB_EXPECT_TRUE(mode != kReserved || lockmode == kShared);
    CALICODB_EXPECT_NE(mode, kPending);

    std::lock_guard guard(inode->mutex);
    if ((lockmode != inode->lock && (inode->lock >= kPending || mode > kShared))) {
        return posix_busy();
    }

    if (mode == kShared && (inode->lock == kShared || inode->lock == kReserved)) {
        // Caller wants a shared lock, and a shared or reserved lock is already
        // held by another thread. Grant the request.
        CALICODB_EXPECT_EQ(mode, kShared);
        CALICODB_EXPECT_EQ(lockmode, kUnlocked);
        CALICODB_EXPECT_GT(inode->nshared, 0);
        lockmode = kShared;
        inode->nshared++;
        inode->nlocks++;
        return Status::ok();
    }
    struct flock lock = {};
    lock.l_len = 1;
    lock.l_whence = SEEK_SET;
    if (mode == kShared || (mode == kExclusive && lockmode == kReserved)) {
        // Attempt to lock the pending byte.
        lock.l_start = kPendingByte;
        lock.l_type = mode == kShared ? F_RDLCK : F_WRLCK;
        if (file_lock(file, lock)) {
            return posix_error(errno);
        } else if (mode == kExclusive) {
            lockmode = kPending;
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
        if (file_lock(file, lock)) {
            s = posix_error(errno);
        }

        // Drop the lock on the pending byte.
        lock.l_start = kPendingByte;
        lock.l_len = 1;
        lock.l_type = F_UNLCK;
        if (file_lock(file, lock) && s.is_ok()) {
            // SQLite says this could happen with a network mount. Such configurations
            // are not yet considered in this design.
            s = posix_error(errno);
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
        return posix_busy();
    } else {
        // The caller is requesting a kReserved or greater. Require that at least a
        // shared lock be held first.
        CALICODB_EXPECT_TRUE(mode == kReserved || mode == kExclusive);
        CALICODB_EXPECT_NE(lockmode, kUnlocked);
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
        if (file_lock(file, lock)) {
            s = posix_error(errno);
        }
    }
    if (s.is_ok()) {
        lockmode = mode;
        inode->lock = mode;
    }
    return s;
}

auto PosixFile::unlock() -> void
{
    struct flock lock = {};

    if (lockmode == kUnlocked) {
        return;
    }
    std::lock_guard guard(inode->mutex);
    CALICODB_EXPECT_NE(inode->nshared, 0);

    if (lockmode > kShared) {
        CALICODB_EXPECT_EQ(inode->lock, lockmode);
        lock.l_type = F_UNLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = kPendingByte;
        // Clear both byte locks (reserved and pending) in the next system call.
        CALICODB_EXPECT_EQ(kPendingByte + 1, kReservedByte);
        lock.l_len = 2;
        // I don't think this really can fail, given that "file" is a valid
        // file descriptor.
        (void)file_lock(file, lock);
        // "file" represents the only file with an exclusive lock, so the inode
        // lock can be downgraded.
        inode->lock = kShared;
    }
    if (--inode->nshared == 0) {
        // The last shared lock has been released.
        lock.l_type = F_UNLCK;
        lock.l_whence = SEEK_SET;
        lock.l_len = 0;
        lock.l_start = 0;
        (void)file_lock(file, lock);
        inode->lock = kUnlocked;
        lockmode = kUnlocked;
    }
    CALICODB_EXPECT_GT(inode->nlocks, 0);

    --inode->nlocks;
    if (inode->nlocks == 0) {
        PosixFs::close_pending_files(*inode);
    }
    lockmode = kUnlocked;
}

} // namespace calicodb
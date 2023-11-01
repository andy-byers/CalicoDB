// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/config.h"
#include "calicodb/env.h"
#include "internal.h"
#include "logging.h"
#include "mem.h"
#include "port.h"
#include "unique_ptr.h"
#include <cerrno>
#include <ctime>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

namespace calicodb
{

namespace
{

class PosixFile;
struct INode;
struct PosixShm;

// Calls the correct form of open()
[[nodiscard]] auto open_wrapper(const char *path, int mode, int permissions) -> int
{
    return ::open(path, mode, permissions);
}

struct SystemCall {
    const char *name;
    void *current;
    void *replace;
} s_syscalls[] = {
    {"open", reinterpret_cast<void *>(open_wrapper), nullptr},
#define sys_open (reinterpret_cast<decltype(open_wrapper) *>(s_syscalls[0].current))

    {"close", reinterpret_cast<void *>(::close), nullptr},
#define sys_close (reinterpret_cast<decltype(::close) *>(s_syscalls[1].current))

    {"access", reinterpret_cast<void *>(::access), nullptr},
#define sys_access (reinterpret_cast<decltype(::access) *>(s_syscalls[2].current))

    {"fstat", reinterpret_cast<void *>(::fstat), nullptr},
#define sys_fstat (reinterpret_cast<decltype(::fstat) *>(s_syscalls[3].current))

    {"ftruncate", reinterpret_cast<void *>(::ftruncate), nullptr},
#define sys_ftruncate (reinterpret_cast<decltype(::ftruncate) *>(s_syscalls[4].current))

    {"fcntl", reinterpret_cast<void *>(::fcntl), nullptr},
#define sys_fcntl (reinterpret_cast<decltype(::fcntl) *>(s_syscalls[5].current))

    {"lseek", reinterpret_cast<void *>(::lseek), nullptr},
#define sys_lseek (reinterpret_cast<decltype(::lseek) *>(s_syscalls[6].current))

    {"read", reinterpret_cast<void *>(::read), nullptr},
#define sys_read (reinterpret_cast<decltype(::read) *>(s_syscalls[7].current))

    {"write", reinterpret_cast<void *>(::write), nullptr},
#define sys_write (reinterpret_cast<decltype(::write) *>(s_syscalls[8].current))

    {"fsync", reinterpret_cast<void *>(::fsync), nullptr},
#define sys_fsync (reinterpret_cast<decltype(::fsync) *>(s_syscalls[9].current))

    {"unlink", reinterpret_cast<void *>(::unlink), nullptr},
#define sys_unlink (reinterpret_cast<decltype(::unlink) *>(s_syscalls[10].current))

    {"mmap", reinterpret_cast<void *>(::mmap), nullptr},
#define sys_mmap (reinterpret_cast<decltype(::mmap) *>(s_syscalls[11].current))

    {"munmap", reinterpret_cast<void *>(::munmap), nullptr},
#define sys_munmap (reinterpret_cast<decltype(::munmap) *>(s_syscalls[12].current))

    {"readlink", reinterpret_cast<void *>(::readlink), nullptr},
#define sys_readlink (reinterpret_cast<decltype(::readlink) *>(s_syscalls[13].current))

    {"lstat", reinterpret_cast<void *>(::lstat), nullptr},
#define sys_lstat (reinterpret_cast<decltype(::lstat) *>(s_syscalls[14].current))

    {"getcwd", reinterpret_cast<void *>(::getcwd), nullptr},
#define sys_getcwd (reinterpret_cast<decltype(::getcwd) *>(s_syscalls[15].current))

    {"stat", reinterpret_cast<void *>(::stat), nullptr},
#define sys_stat (reinterpret_cast<decltype(::stat) *>(s_syscalls[16].current))
};

class PosixEnv
    : public Env,
      public HeapObject
{
    uint16_t m_rng[3] = {};

public:
    explicit PosixEnv();
    ~PosixEnv() override = default;

    [[nodiscard]] auto max_filename() const -> size_t override;
    auto full_filename(const char *filename, char *out, size_t out_size) -> Status override;
    auto new_logger(const char *filename, Logger *&out) -> Status override;
    auto new_file(const char *filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto file_exists(const char *filename) const -> bool override;
    auto remove_file(const char *filename) -> Status override;

    void srand(unsigned seed) override;
    [[nodiscard]] auto rand() -> unsigned override;

    void sleep(unsigned micros) override;
};

constexpr size_t kPathMax = 512;       // Maximum path length from SQLite.
constexpr int kFilePermissions = 0644; // -rw-r--r--
constexpr size_t kMaxSymlinks = 100;

// Constants for SQLite-style shared memory locking
// There are "File::kShmLockCount" lock bytes available. See include/calicodb/env.h
// for more details.
constexpr size_t kShmLock0 = 120;
constexpr size_t kShmDMS = kShmLock0 + File::kShmLockCount;

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
    uint64_t inode = 0;
};

struct ShmNode final {
    INode *inode = nullptr;

    mutable port::Mutex mutex;

    String filename;
    int file = -1;

    bool is_unlocked = false;

    // 32-KB blocks of shared memory.
    Vector<char *> regions;

    // List of PosixShm objects that reference this ShmNode.
    PosixShm *refs = nullptr;
    size_t refcount = 0;

    // Locks held by PosixShm instances in this process. 0 means unlocked, -1 an
    // exclusive lock, and a positive integer N means that N shared locks are
    // held.
    int locks[File::kShmLockCount] = {};

    ~ShmNode();

    // Lock the DMS ("dead man switch") byte
    // A reader lock is held on the DMS byte by each shared memory connection.
    // When a connection is dropped, the reader lock is released. A connection
    // knows it is the first connection if it can get a writer lock on the DMS
    // byte.
    [[nodiscard]] auto take_dms_lock() -> int;
    [[nodiscard]] auto check_locks() const -> bool;
};

struct UnusedFd final {
    int file;
    int mode;
    UnusedFd *next;
};

struct INode final {
    FileId key;
    mutable port::Mutex mutex;
    unsigned nlocks = 0;
    int lock = kLockUnlocked;

    // List of file descriptors that are waiting to be closed.
    UnusedFd *unused = nullptr;

    // Number of PosixFile instances referencing this inode.
    int refcount = 0;

    // If this inode has had shared memory opened on it, then a pointer to a heap-
    // allocated ShmNode will be stored here. It will be cleaned up when its
    // refcount goes to 0.
    ObjectPtr<ShmNode> snode;

    // Linked list of inodes. Stored as raw pointers so that inodes can be removed
    // from the list in constant time without having to store std::list iterators
    // in the PosixFile class.
    INode *next = nullptr;
    INode *prev = nullptr;
};

auto posix_file_lock(int file, const struct flock &lock) -> int
{
    const auto rc = sys_fcntl(file, F_SETLK, &lock);
    if (rc < 0 && errno == EACCES) {
        // Either EACCES or EAGAIN is set when fcntl() detects that a conflicting lock is held by
        // another process. open() also sets EACCES due to inadequate permissions, so convert to a
        // different error code to avoid ambiguity (EAGAIN already converts to a busy status).
        // Source: https://man7.org/linux/man-pages/man2/fcntl.2.html
        errno = EAGAIN;
    }
    return rc;
}

[[nodiscard]] auto posix_shm_lock(ShmNode &snode, short type, size_t offset, size_t n) -> int
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

[[nodiscard]] auto posix_error(int error) -> Status
{
    CALICODB_EXPECT_NE(error, 0);
    switch (error) {
        case EAGAIN:
        case EBUSY:
        case EINTR:
        case ENOLCK:
        case ETIMEDOUT:
            return Status::busy(std::strerror(error));
        case ENOENT:
            return Status::not_found(std::strerror(error));
        default:
            return Status::io_error(std::strerror(error));
    }
}

constexpr size_t kInterruptTimeout = 100;

[[nodiscard]] auto posix_open(const char *filename, int mode) -> int
{
    for (size_t t = 0; t < kInterruptTimeout; ++t) {
        const auto fd = sys_open(filename, mode | O_CLOEXEC, kFilePermissions);
        if (fd < 0 && errno == EINTR) {
            continue;
        }
        return fd;
    }
    return -1;
}

auto posix_close(int fd) -> int
{
    for (size_t t = 0; t < kInterruptTimeout; ++t) {
        const auto rc = sys_close(fd);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        return rc;
    }
    return -1;
}

[[nodiscard]] auto posix_read(int file, size_t size, char *scratch, Slice *out) -> int
{
    auto rest = size;
    while (rest > 0) {
        const auto n = sys_read(file, scratch, rest);
        if (n <= 0) {
            if (n == 0) {
                break;
            } else if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        rest -= static_cast<size_t>(n);
    }
    std::memset(scratch + size - rest, 0, rest);
    if (out != nullptr) {
        *out = Slice(scratch, size - rest);
    }
    return 0;
}

auto posix_write(int file, Slice in) -> int
{
    while (!in.is_empty()) {
        const auto n = sys_write(file, in.data(), in.size());
        if (n >= 0) {
            in.advance(static_cast<size_t>(n));
        } else if (errno != EINTR) {
            return -1;
        }
    }
    return 0;
}

[[nodiscard]] auto seek_and_read(int file, size_t offset, size_t size, char *scratch, Slice *out) -> int
{
    if (const auto rc = sys_lseek(file, static_cast<off_t>(offset), SEEK_SET); rc < 0) {
        return -1;
    }
    return posix_read(file, size, scratch, out);
}

[[nodiscard]] auto seek_and_write(int file, size_t offset, Slice in) -> int
{
    if (const auto rc = sys_lseek(file, static_cast<off_t>(offset), SEEK_SET); rc < 0) {
        return -1;
    }
    return posix_write(file, in);
}

[[nodiscard]] auto posix_truncate(int fd, size_t size) -> int
{
    for (size_t t = 0; t < kInterruptTimeout; ++t) {
        const auto rc = sys_ftruncate(fd, static_cast<off_t>(size));
        if (rc && errno == EINTR) {
            continue;
        }
        return rc;
    }
    return -1;
}

struct PathHelper {
    Status s;
    size_t symlinks;
    char *output;
    size_t size;
    size_t used;
};

void append_elements(PathHelper &path, const char *elements);

void append_one_element(PathHelper &path, const char *name, size_t size)
{
    CALICODB_EXPECT_GT(size, 0);
    CALICODB_EXPECT_NE(name, nullptr);
    if (name[0] == '.') {
        if (size == 1) {
            return; // Current directory: NOOP
        }
        if (name[1] == '.' && size == 2) {
            if (path.used > 1) {
                // Parent directory: pop the last element
                CALICODB_EXPECT_EQ(path.output[0], '/');
                while (path.output[--path.used] != '/') {
                }
            }
            return;
        }
    }
    if (path.used + size + 2 >= path.size) {
        path.s = Status::invalid_argument("path is too long");
        return;
    }
    path.output[path.used++] = '/';
    std::memcpy(path.output + path.used, name, size);
    path.used += size;

    if (!path.s.is_ok()) {
        return;
    }
    struct stat st;
    path.output[path.used] = '\0';
    const auto *input = path.output;
    if (sys_lstat(input, &st)) {
        if (errno != ENOENT) {
            path.s = posix_error(errno);
        }
    } else if (S_ISLNK(st.st_mode)) {
        char link[kPathMax + 2];
        if (path.symlinks++ > kMaxSymlinks) {
            path.s = Status::invalid_argument();
        }
        const auto got = sys_readlink(input, link, kPathMax);
        if (got <= 0 || got >= static_cast<ssize_t>(kPathMax)) {
            path.s = Status::io_error("readlink");
            return;
        }
        link[got] = '\0';
        if (link[0] == '/') {
            path.used = 0;
        } else {
            path.used -= size + 1;
        }
        append_elements(path, link);
    }
}

void append_elements(PathHelper &path, const char *elements)
{
    size_t i = 0;
    size_t j = 0;
    do {
        while (elements[i] && elements[i] != '/') {
            ++i;
        }
        if (i > j) {
            append_one_element(path, elements + j, i - j);
        }
        j = i + 1;
    } while (elements[i++]);
}

struct PosixShm {
    auto lock(size_t r, size_t n, ShmLockFlag flags) -> Status;
    auto lock_impl(size_t r, size_t n, ShmLockFlag flags) -> Status;

    ShmNode *snode = nullptr;
    PosixShm *next = nullptr;
    uint16_t reader_mask = 0;
    uint16_t writer_mask = 0;
};

#define READ_WRITE_MODE(mode) (static_cast<int>(mode) & (Env::kReadOnly | Env::kReadWrite))

class PosixFile
    : public File,
      public HeapObject
{
public:
    explicit PosixFile(Env &env, String filename, int mode, UniquePtr<UnusedFd> prealloc)
        : filename(move(filename)),
          prealloc(move(prealloc)),
          env(&env),
          rw_mode(mode)
    {
    }

    ~PosixFile() override
    {
        (void)close();
    }

    auto close() -> Status;

    auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status override;
    auto write(uint64_t offset, const Slice &in) -> Status override;
    auto get_size(uint64_t &size_out) const -> Status override;
    auto resize(uint64_t size) -> Status override;
    auto sync() -> Status override;
    auto file_lock(FileLockMode mode) -> Status override;
    void file_unlock() override;

    auto shm_map(size_t r, bool extend, volatile void *&out) -> Status override;
    auto shm_lock(size_t r, size_t n, ShmLockFlag flags) -> Status override;
    void shm_unmap(bool unlink) override;
    void shm_barrier() override;

    auto file_lock_impl(FileLockMode mode) -> Status;

    String filename;
    UniquePtr<UnusedFd> prealloc;
    ObjectPtr<PosixShm> shm;
    INode *inode = nullptr;
    Env *const env;
    int rw_mode = 0;
    int file = -1;

    // Lock mode for this particular file descriptor.
    int local_lock = 0;
};

class PosixLogger
    : public Logger,
      public HeapObject
{
    int m_file;

public:
    explicit PosixLogger(int file)
        : m_file(file)
    {
    }

    ~PosixLogger() override
    {
        (void)posix_close(m_file);
    }

    void append(const Slice &msg) override
    {
        posix_write(m_file, msg);
    }

    // Modified from LevelDB.
    void logv(const char *fmt, std::va_list args) override
    {
        timeval now_tv;
        std::tm now_tm;
        gettimeofday(&now_tv, nullptr);
        localtime_r(&now_tv.tv_sec, &now_tm);

        char *var = nullptr;
        char fix[256];
        auto *p = fix;
        auto L = sizeof(fix);

        for (int i = 0; i < 2; ++i) {
            auto offset = static_cast<size_t>(std::snprintf(
                p, L, "%04d/%02d/%02d-%02d:%02d:%02d.%06d ",
                now_tm.tm_year + 1900, now_tm.tm_mon + 1,
                now_tm.tm_mday, now_tm.tm_hour, now_tm.tm_min,
                now_tm.tm_sec, static_cast<int>(now_tv.tv_usec)));

            std::va_list args_copy;
            va_copy(args_copy, args);
            offset += static_cast<size_t>(std::vsnprintf(
                p + offset, L - offset, fmt, args_copy));
            va_end(args_copy);

            if (offset + 1 >= L) {
                if (i == 0) {
                    L = offset + 2; // Account for '\n'
                    var = static_cast<char *>(Mem::allocate(L));
                    if (var == nullptr) {
                        // Write just this generic message if the system could not fulfill the allocation.
                        static constexpr const char kNoMemory[] = "could not log message: no memory for temp buffer\n";
                        append(Slice(kNoMemory, ARRAY_SIZE(kNoMemory)));
                        return;
                    }
                    p = var;
                    continue;
                }
                offset = L - 1;
            }
            if (p[offset - 1] != '\n') {
                p[offset++] = '\n';
            }
            append(Slice(p, offset));
            break;
        }
        Mem::deallocate(var);
    }
};

// Per-process singleton for managing filesystem state
struct PosixFs final {
    explicit PosixFs()
        : page_size(static_cast<size_t>(sysconf(_SC_PAGESIZE)))
    {
        if (page_size < File::kShmRegionSize) {
            mmap_scale = 1;
        } else {
            mmap_scale = page_size / File::kShmRegionSize;
        }
    }

    static void close_pending_files(INode &inode)
    {
        // REQUIRES: Mutex "inode.mutex" is locked by the caller
        for (auto *file = inode.unused; file;) {
            auto *next = file->next;
            posix_close(file->file);
            Mem::deallocate(file);
            file = next;
        }
        inode.unused = nullptr;
    }

    [[nodiscard]] auto find_unused_fd(const char *path, int flags) -> UnusedFd *
    {
        UnusedFd *unused = nullptr;
        struct stat st;
        mutex.lock();

        if (inode_list && 0 == sys_stat(path, &st)) {
            auto *inode = inode_list;
            while (inode && (inode->key.device != st.st_dev ||
                             inode->key.inode != st.st_ino)) {
                inode = inode->next;
            }
            if (inode) {
                inode->mutex.lock();
                flags &= Env::kReadOnly | Env::kReadWrite;
                UnusedFd **ptr;
                // Seek ptr to the first file descriptor with matching read/write mode.
                for (ptr = &inode->unused;
                     *ptr && (*ptr)->mode != flags;
                     ptr = &(*ptr)->next) {
                }
                unused = *ptr;
                if (unused) {
                    *ptr = unused->next;
                }
                inode->mutex.unlock();
            }
        }
        mutex.unlock();
        return unused;
    }

    [[nodiscard]] auto ref_inode(int fd, INode *&ino_out) -> Status
    {
        // REQUIRES: "mutex" member is locked by the caller
        FileId key;
        if (struct stat st = {}; sys_fstat(fd, &st)) {
            return posix_error(errno);
        } else {
            key.device = st.st_dev;
            key.inode = st.st_ino;
        }

        auto *ino = inode_list;
        while (ino && key != ino->key) {
            ino = ino->next;
        }
        if (ino == nullptr) {
            ino = Mem::new_object<INode>();
            if (ino == nullptr) {
                return Status::no_memory();
            }
            ino->key = key;
            ino->refcount = 1;
            ino->next = inode_list;
            ino->prev = nullptr;
            if (inode_list) {
                inode_list->prev = ino;
            }
            inode_list = ino;
        } else {
            ++ino->refcount;
        }
        ino_out = ino;
        return Status::ok();
    }

    void unref_inode(INode *&inode)
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
                CALICODB_EXPECT_EQ(inode_list, inode);
                inode_list = inode->next;
            }
            if (inode->next) {
                CALICODB_EXPECT_EQ(inode->next->prev, inode);
                inode->next->prev = inode->prev;
            }
            Mem::delete_object(inode);
        }
        inode = nullptr;
    }

    auto ref_snode(PosixFile &file, PosixShm *&shm_out) const -> Status
    {
        ObjectPtr<PosixShm> shm_storage(Mem::new_object<PosixShm>());
        if (!shm_storage) {
            return Status::no_memory();
        }
        mutex.lock();
        Status s;
        auto *inode = file.inode;
        auto *snode = inode->snode.get();
        if (snode == nullptr) {
            s = Status::no_memory();
            // Allocate storage for the shm node.
            ObjectPtr<ShmNode> new_snode(Mem::new_object<ShmNode>());
            if (!new_snode) {
                goto cleanup;
            }

            // Allocate storage for the shm filename.
            if (append_strings(
                    new_snode->filename,
                    Slice(file.filename),
                    kDefaultShmSuffix)) {
                goto cleanup;
            }

            // Open the shm file.
            new_snode->file = posix_open(
                new_snode->filename.c_str(),
                O_CREAT | O_NOFOLLOW | O_RDWR);
            if (new_snode->file < 0) {
                s = posix_error(errno);
                goto cleanup;
            }
            // WARNING: If another process unlinks the file after we opened it above, the
            //          attempt to take the DMS lock here will fail.
            if (new_snode->take_dms_lock()) {
                s = Status::busy();
                goto cleanup;
            }

            snode = new_snode.get();
            inode->snode = move(new_snode);
            snode->inode = inode;
            s = Status::ok();
        }
        CALICODB_EXPECT_GE(snode->file, 0);
        ++snode->refcount;

    cleanup:
        mutex.unlock();
        if (!s.is_ok()) {
            return s;
        }

        snode->mutex.lock();
        shm_out = shm_storage.release();
        shm_out->snode = snode;
        shm_out->next = snode->refs;
        snode->refs = shm_out;
        snode->mutex.unlock();
        return Status::ok();
    }

    void unref_snode(PosixShm &shm, bool unlink_if_last) const
    {
        auto *snode = shm.snode;
        auto *inode = snode->inode;

        snode->mutex.lock();
        auto **pp = &snode->refs;
        for (; *pp != &shm; pp = &(*pp)->next) {
        }
        // Remove "shm" from the list.
        *pp = shm.next;
        snode->mutex.unlock();

        // Global mutex must be locked when creating or destroying shm nodes.
        mutex.lock();
        CALICODB_EXPECT_GT(snode->refcount, 0);

        if (--snode->refcount == 0) {
            const auto interval = mmap_scale;
            for (size_t i = 0; i < snode->regions.size(); i += interval) {
                sys_munmap(snode->regions[i], File::kShmRegionSize);
            }
            if (unlink_if_last) {
                // Take a write lock on the DMS byte to make sure no other processes are
                // using this shm file.
                if (0 == posix_shm_lock(*snode, F_WRLCK, kShmDMS, 1)) {
                    // This should drop the lock we just took.
                    sys_unlink(snode->filename.c_str());
                }
            }
            inode->snode.reset();
        }
        mutex.unlock();
    }

    // Linked list of inodes, protected by a mutex.
    mutable port::Mutex mutex;
    INode *inode_list = nullptr;

    // OS page size for mmap().
    size_t page_size = 0;

    // The OS page size may be greater than the shared memory region
    // size (File::kShmRegionSize). If so, then mmap() must allocate this
    // number of regions each time it is called.
    size_t mmap_scale = 0;
} s_fs;

void seed_prng_state(uint16_t *state, uint32_t seed)
{
    state[0] = 0x330E;
    std::memcpy(&state[1], &seed, sizeof(seed));
}

auto open_parent_dir(const char *filename, int &fd_out) -> int
{
    char dirname[kPathMax + 1];
    fd_out = -1;

    std::snprintf(dirname, sizeof(dirname), "%s", filename);
    auto i = std::strlen(dirname);
    while (i > 0 && dirname[i] != '/') {
        --i;
    }
    if (i > 0) {
        dirname[i] = '\0';
    } else {
        if (dirname[0] != '/') {
            dirname[0] = '.';
        }
        dirname[1] = '\0';
    }
    fd_out = posix_open(dirname, O_RDONLY);
    return fd_out < 0 ? -1 : 0;
}

void sync_parent_dir(const char *filename)
{
    int dir;
    if (open_parent_dir(filename, dir)) {
        return;
    }
    if (sys_fsync(dir)) {
        // TODO: Use a default logger, log errors like this one.
    }
    posix_close(dir);
}

// Env constructor is not allowed to allocate memory: the allocation subsystem may not be set
// up yet, since this runs during static initialization while creating the default Env instance.
PosixEnv::PosixEnv()
{
    seed_prng_state(m_rng, static_cast<uint32_t>(time(nullptr)));
}

auto PosixEnv::max_filename() const -> size_t
{
    return kPathMax;
}

auto PosixEnv::full_filename(const char *filename, char *out, size_t out_size) -> Status
{
    PathHelper path = {Status::ok(), 0, out, out_size, 0};
    if (filename[0] != '/') {
        char pwd[kPathMax + 2];
        if (!sys_getcwd(pwd, kPathMax)) {
            return posix_error(errno);
        }
        append_elements(path, pwd);
    }
    append_elements(path, filename);
    out[path.used] = '\0';
    if (path.used < 2) {
        return Status::invalid_argument();
    }
    return path.s;
}

auto PosixEnv::remove_file(const char *filename) -> Status
{
    if (sys_unlink(filename)) {
        return posix_error(errno);
    }
    sync_parent_dir(filename);
    return Status::ok();
}

auto PosixEnv::file_exists(const char *filename) const -> bool
{
    return sys_access(filename, F_OK) == 0;
}

auto PosixEnv::new_file(const char *filename, OpenMode mode, File *&out) -> Status
{
    UnusedFd *reuse;
    out = nullptr;

    const auto flags =
        ((mode & kCreate) ? O_CREAT : 0) |
        ((mode & kReadOnly) ? O_RDONLY : O_RDWR);

    // Allocate storage for the filename. Alloc::to_string() adds a '\0'.
    const Slice filename_slice(filename, std::strlen(filename));
    String filename_storage;
    if (append_strings(filename_storage, filename_slice)) {
        return Status::no_memory();
    }

    // Allocate storage for an UnusedFd.
    UniquePtr<UnusedFd> prealloc(static_cast<UnusedFd *>(
        Mem::allocate(sizeof(UnusedFd))));
    if (!prealloc) {
        return Status::no_memory();
    }

    auto s = Status::no_memory();
    INode *inode = nullptr;

    // Allocate storage for the File instance. Files are exposed to the user, so just use regular
    // operator new(). The Alloc API is only used for internal structures.
    auto *file = new (std::nothrow) PosixFile(
        *this,
        move(filename_storage),
        READ_WRITE_MODE(mode),
        move(prealloc));
    if (file == nullptr) {
        goto cleanup;
    }

    reuse = s_fs.find_unused_fd(filename, mode);
    if (reuse) {
        // Reuse a file descriptor opened by another connection.
        file->file = reuse->file;
        Mem::deallocate(reuse);
    } else {
        // Open the file. Let the OS choose what file descriptor to use.
        file->file = posix_open(filename, flags);
        if (file->file < 0) {
            s = posix_error(errno);
            goto cleanup;
        }
    }
    CALICODB_EXPECT_GE(file->file, 0);

    // Search the global inode info list. This requires locking the global mutex.
    s_fs.mutex.lock();
    s = s_fs.ref_inode(file->file, inode);
    s_fs.mutex.unlock();
    if (s.is_ok()) {
        file->inode = inode;
        out = file;
    }

cleanup:
    if (!s.is_ok()) {
        delete file;
    }
    return s;
}

auto PosixEnv::new_logger(const char *filename, Logger *&out) -> Status
{
    const auto file = posix_open(filename, O_CREAT | O_WRONLY | O_APPEND);
    if (file < 0) {
        return posix_error(errno);
    }
    Status s;
    out = new (std::nothrow) PosixLogger(file);
    if (out == nullptr) {
        posix_close(file);
        s = Status::no_memory();
    }
    return s;
}

void PosixEnv::srand(unsigned seed)
{
    seed_prng_state(m_rng, seed);
}

auto PosixEnv::rand() -> unsigned
{
    return static_cast<unsigned>(nrand48(m_rng));
}

void PosixEnv::sleep(unsigned micros)
{
    static constexpr unsigned kMicrosPerSecond = 1'000'000;
    if (micros >= kMicrosPerSecond) {
        ::sleep(micros / kMicrosPerSecond);
    }
    if (micros % kMicrosPerSecond) {
        ::usleep(micros % kMicrosPerSecond);
    }
}

auto PosixFile::get_size(uint64_t &size_out) const -> Status
{
    struct stat st;
    if (sys_fstat(file, &st)) {
        return posix_error(errno);
    }
    size_out = static_cast<uint64_t>(st.st_size);
    return Status::ok();
}

auto PosixFile::close() -> Status
{
    auto fd = file;
    file = -1;

    if (fd < 0) {
        // Already closed. NOOP.
        return Status::ok();
    } else if (inode == nullptr) {
        // Opened the file, but failed to allocate the INode structure. Just close
        // the file.
        if (posix_close(fd)) {
            return posix_error(errno);
        }
        return Status::ok();
    }
    CALICODB_EXPECT_TRUE(inode);
    CALICODB_EXPECT_FALSE(shm);
    file_unlock();

    s_fs.mutex.lock();
    inode->mutex.lock();

    if (inode->nlocks) {
        // Some other thread in this process has a lock on this file from a different
        // file descriptor. Callinc close() on this file descriptor will cause other
        // threads to lose their locks. Defer close() until the other locks have been
        // released. This uses the UnusedFd created when the file was opened.
        prealloc->file = fd;
        prealloc->mode = rw_mode;
        prealloc->next = inode->unused;
        inode->unused = prealloc.release();
        fd = -1;
    }
    inode->mutex.unlock();
    s_fs.unref_inode(inode);
    s_fs.mutex.unlock();

    if (fd >= 0 && posix_close(fd)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status
{
    if (seek_and_read(file, offset, size, scratch, out)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::write(uint64_t offset, const Slice &in) -> Status
{
    if (seek_and_write(file, offset, in)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::resize(uint64_t size) -> Status
{
    if (posix_truncate(file, size)) {
        return posix_error(errno);
    }
    return Status::ok();
}

auto PosixFile::sync() -> Status
{
    int rc;

#ifdef F_FULLFSYNC
    // NOTE: This will certainly make performance quite a bit worse on macOS, however, it is
    //       necessary for durability. On macOS, fcntl() returns before the storage device's
    //       volatile write cache has been flushed.
    rc = sys_fcntl(file, F_FULLFSYNC);
#else
    rc = 1;
#endif

    if (rc) {
        rc = sys_fsync(file);
    }
    return rc ? posix_error(errno) : Status::ok();
}

void PosixFile::shm_unmap(bool unlink)
{
    if (shm) {
        s_fs.unref_snode(*shm, unlink);
        shm.reset();
    }
}

auto PosixFile::shm_map(size_t r, bool extend, volatile void *&out) -> Status
{
    out = nullptr;
    if (!shm) {
        auto s = s_fs.ref_snode(*this, shm.ref());
        if (!s.is_ok()) {
            return s;
        }
    }
    CALICODB_EXPECT_TRUE(shm);
    auto *snode = shm->snode;
    CALICODB_EXPECT_TRUE(snode);

    // Determine the file size (in shared memory regions) needed to satisfy the
    // request for region "r".
    const auto mmap_scale = s_fs.mmap_scale;
    const auto request = (r + mmap_scale) / mmap_scale * mmap_scale;

    Status s;
    snode->mutex.lock();
    if (snode->is_unlocked) {
        if (snode->take_dms_lock()) {
            s = posix_error(EAGAIN);
            goto cleanup;
        }
        snode->is_unlocked = false;
    }

    if (snode->regions.size() < request) {
        uint64_t file_size;
        if (struct stat st = {}; sys_fstat(snode->file, &st)) {
            s = posix_error(errno);
            goto cleanup;
        } else {
            file_size = static_cast<size_t>(st.st_size);
        }
        if (file_size < request * File::kShmRegionSize) {
            if (!extend) {
                goto cleanup;
            }
            // Write a '\0' to the end of the highest-addressed region to extend the
            // file. SQLite writes a byte to the end of each OS page, causing the pages
            // to be allocated immediately (to reduce the chance of a later SIGBUS).
            // This should be good enough for now.
            if (seek_and_write(snode->file,
                               request * File::kShmRegionSize - 1,
                               Slice("", 1))) {
                s = posix_error(errno);
                goto cleanup;
            }
        }

        snode->regions.reserve(request);
        while (snode->regions.size() < request) {
            // Map "mmap_scale" shared memory regions into this address space.
            auto *p = sys_mmap(
                nullptr, File::kShmRegionSize * mmap_scale,
                PROT_READ | PROT_WRITE, MAP_SHARED, snode->file,
                static_cast<ssize_t>(File::kShmRegionSize * snode->regions.size()));
            if (p == MAP_FAILED) {
                s = posix_error(errno);
                break;
            }
            // Store a pointer to the start of each memory region.
            for (size_t i = 0; i < mmap_scale; ++i) {
                if (snode->regions.push_back(static_cast<char *>(p) +
                                             File::kShmRegionSize * i)) {
                    s = Status::no_memory();
                    goto cleanup;
                }
            }
        }
    }

cleanup:
    if (r < snode->regions.size()) {
        out = snode->regions[r];
    }
    snode->mutex.unlock();
    return s;
}

auto PosixFile::shm_lock(size_t r, size_t n, ShmLockFlag flags) -> Status
{
    if (shm) {
        return shm->lock(r, n, flags);
    }
    return Status::io_error("shm is unmapped");
}

void PosixFile::shm_barrier()
{
    CALICODB_DEBUG_DELAY(*env);

    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    s_fs.mutex.lock();
    s_fs.mutex.unlock();
}

auto PosixShm::lock(size_t r, size_t n, ShmLockFlag flags) -> Status
{
    CALICODB_EXPECT_LE(r + n, File::kShmLockCount);
    CALICODB_EXPECT_GT(n, 0);
    CALICODB_EXPECT_TRUE(
        flags == (kShmLock | kShmReader) ||
        flags == (kShmLock | kShmWriter) ||
        flags == (kShmUnlock | kShmReader) ||
        flags == (kShmUnlock | kShmWriter));
    CALICODB_EXPECT_TRUE(n == 1 || (flags & kShmWriter));

    snode->mutex.lock();
    auto s = lock_impl(r, n, flags);
    snode->mutex.unlock();
    return s;
}

auto PosixShm::lock_impl(size_t r, size_t n, ShmLockFlag flags) -> Status
{
    auto *state = snode->locks;
    const auto mask = static_cast<uint16_t>((1 << (r + n)) - (1 << r));
    CALICODB_EXPECT_TRUE(n > 1 || mask == (1 << r));
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
                return Status::busy();
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
        for (size_t i = r; i < r + n; ++i) {
            if ((writer_mask & (1 << i)) == 0 && state[i]) {
                // Some other thread in this process has a lock.
                return Status::busy();
            }
        }

        if (posix_shm_lock(*snode, F_WRLCK, r + kShmLock0, n)) {
            // Some thread in another process has a lock.
            return posix_error(errno);
        }
        CALICODB_EXPECT_FALSE(reader_mask & mask);
        for (size_t i = 0; i < n; ++i) {
            state[r + i] = -1;
        }
        writer_mask |= mask;
    }
    CALICODB_EXPECT_TRUE(snode->check_locks());
    return Status::ok();
}

ShmNode::~ShmNode()
{
    (void)posix_close(file);
}

auto ShmNode::take_dms_lock() -> int
{
    struct flock lock = {};
    lock.l_whence = SEEK_SET;
    lock.l_start = kShmDMS;
    lock.l_len = 1;
    lock.l_type = F_WRLCK;

    int rc = 0;
    if (sys_fcntl(file, F_GETLK, &lock)) {
        rc = -1;
    } else if (lock.l_type == F_UNLCK) {
        // The DMS byte is unlocked, meaning this must be the first connection.
        rc = posix_shm_lock(*this, F_WRLCK, kShmDMS, 1);
        if (rc == 0) {
            rc = posix_truncate(file, 0);
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

auto ShmNode::check_locks() const -> bool
{
#ifdef CALICODB_TEST
    // REQUIRES: "snode->mutex" is locked
    int check[File::kShmLockCount] = {};

    for (auto *p = refs; p; p = p->next) {
        for (size_t i = 0; i < File::kShmLockCount; ++i) {
            if (p->writer_mask & (1 << i)) {
                CALICODB_EXPECT_EQ(check[i], 0);
                check[i] = -1;
            } else if (p->reader_mask & (1 << i)) {
                CALICODB_EXPECT_GE(check[i], 0);
                ++check[i];
            }
        }
    }
    for (size_t i = 0; i < ARRAY_SIZE(check); ++i) {
        CALICODB_EXPECT_EQ(check[i], locks[i]);
    }
#endif // CALICODB_TEST
    return true;
}

auto PosixFile::file_lock(FileLockMode mode) -> Status
{
    if (mode <= local_lock) {
        return Status::ok();
    }
    // First lock taken on a file must be kShared.
    CALICODB_EXPECT_TRUE(local_lock != kLockUnlocked || mode == kFileShared);

    inode->mutex.lock();
    auto s = file_lock_impl(mode);
    inode->mutex.unlock();
    return s;
}

auto PosixFile::file_lock_impl(FileLockMode mode) -> Status
{
    if (local_lock != inode->lock && (inode->lock == kFileExclusive || mode == kFileExclusive)) {
        // Some other thread in this process has an incompatible lock.
        return Status::busy();
    }

    if (mode == kFileShared && inode->lock == kFileShared) {
        // Caller wants a shared lock, and a shared lock is already held by another thread.
        // Grant the lock. This block is just to avoid actually calling out to fcntl(),
        // since we already know this lock is compatible.
        CALICODB_EXPECT_EQ(local_lock, kLockUnlocked);
        CALICODB_EXPECT_GT(inode->nlocks, 0);
        local_lock = kFileShared;
        inode->nlocks++;
        return Status::ok();
    }
    struct flock lock = {};
    lock.l_len = 0;
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;

    Status s;
    if (mode == kFileShared) {
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
    } else if (mode == kFileExclusive && inode->nlocks > 1) {
        // Another thread in this process still holds a shared lock, preventing
        // this kExclusive from being taken. Note that this thread should already
        // have a shared lock (guarded for by an assert).
        s = Status::busy();
    } else {
        // The caller is requesting an exclusive lock, and no other thread in
        // this process already holds a lock.
        CALICODB_EXPECT_EQ(mode, kFileExclusive);
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

void PosixFile::file_unlock()
{
    if (local_lock == kLockUnlocked) {
        return;
    }

    struct flock lock = {};
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;

    inode->mutex.lock();
    CALICODB_EXPECT_TRUE(inode->lock == kFileShared || inode->nlocks == 1);
    CALICODB_EXPECT_GT(inode->nlocks, 0);

    if (--inode->nlocks == 0) {
        posix_file_lock(file, lock);
        PosixFs::close_pending_files(*inode);
        inode->lock = kLockUnlocked;
    }
    local_lock = kLockUnlocked;
    inode->mutex.unlock();
}

} // namespace

auto default_env() -> Env &
{
    static PosixEnv s_env;
    return s_env;
}

auto replace_syscall(const SyscallConfig &config) -> Status
{
    if (config.syscall == nullptr) {
        return Status::invalid_argument("syscall pointer is null");
    }
    for (auto &saved : s_syscalls) {
        if (0 == std::strcmp(config.name, saved.name)) {
            if (saved.replace == nullptr) {
                saved.replace = saved.current;
            }
            saved.current = config.syscall;
            return Status::ok();
        }
    }
    return Status::invalid_argument("unrecognized syscall");
}

auto restore_syscall(const char *name) -> Status
{
    for (auto &saved : s_syscalls) {
        if (0 == std::strcmp(name, saved.name)) {
            if (saved.replace) {
                saved.current = saved.replace;
                saved.replace = nullptr;
            }
            return Status::ok();
        }
    }
    return Status::invalid_argument("unrecognized syscall");
}

} // namespace calicodb

#include "db_impl.h"
#include "calicodb/calicodb.h"
#include "calicodb/env.h"
#include "crc.h"
#include "env_posix.h"
#include "logging.h"

namespace calicodb
{

#define SET_STATUS(s)           \
    do {                        \
        if (m_status.is_ok()) { \
            m_status = s;       \
        }                       \
    } while (0)

[[nodiscard]] static auto sanitize_options(const Options &options) -> Options
{
    auto sanitized = options;
    if (sanitized.cache_size == 0) {
        sanitized.cache_size = options.page_size * 64;
    }
    return sanitized;
}

auto DBImpl::open(const Options &options, const Slice &filename) -> Status
{
    if (filename.is_empty()) {
        return Status::invalid_argument("path is empty");
    }
    auto sanitized = sanitize_options(options);

    m_filename = filename.to_string();
    const auto [dir, base] = split_path(m_filename);
    m_filename = join_paths(dir, base);

    m_wal_prefix = sanitized.wal_prefix;
    if (m_wal_prefix.empty()) {
        m_wal_prefix = m_filename + kDefaultWalSuffix;
    }

    // Any error during initialization is fatal.
    return do_open(sanitized);
}

auto DBImpl::do_open(Options sanitized) -> Status
{
    m_env = sanitized.env;
    if (m_env == nullptr) {
        m_env = Env::default_env();
        m_owns_env = true;
    }

    if (auto s = m_env->file_exists(m_filename); s.is_not_found()) {
        if (!sanitized.create_if_missing) {
            return Status::invalid_argument("database does not exist");
        }
    } else if (s.is_ok()) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument("database already exists");
        }
    } else {
        return s;
    }

    m_info_log = sanitized.info_log;
    if (m_info_log == nullptr) {
        CDB_TRY(m_env->new_info_logger(m_filename + kDefaultLogSuffix, &m_info_log));
        sanitized.info_log = m_info_log;
        m_owns_info_log = true;
    }

    FileHeader state;
    CDB_TRY(setup(m_filename, *m_env, sanitized, state));
    m_commit_lsn = state.commit_lsn;
    m_record_count = state.record_count;
    if (!m_commit_lsn.is_null()) {
        sanitized.page_size = state.page_size;
    }
    m_scratch.resize(wal_scratch_size(sanitized.page_size));

    CDB_TRY(WriteAheadLog::open(
        {
            m_wal_prefix,
            m_env,
            sanitized.page_size,
        },
        &wal));

    CDB_TRY(Pager::open(
        {
            m_filename,
            m_env,
            &m_scratch,
            wal,
            m_info_log,
            &m_status,
            &m_commit_lsn,
            &m_in_txn,
            sanitized.cache_size / sanitized.page_size,
            sanitized.page_size,
        },
        &pager));
    pager->load_state(state);

    tree = new Tree {*pager, &m_freelist_head};
    tree->load_state(state);

    Status s;
    if (m_commit_lsn.is_null()) {
        m_info_log->logv("setting up a new database");
        CDB_TRY(wal->start_writing());
        CDB_TRY(Tree::create(*pager, &m_freelist_head));
        CDB_TRY(do_commit());
        CDB_TRY(pager->flush());

    } else {
        m_info_log->logv("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CDB_TRY(ensure_consistency());
        CDB_TRY(load_state());
        CDB_TRY(wal->start_writing());
    }
    m_info_log->logv("pager recovery lsn is %llu", pager->recovery_lsn().value);
    m_info_log->logv("wal flushed lsn is %llu", wal->flushed_lsn().value);
    m_info_log->logv("commit lsn is %llu", m_commit_lsn.value);

    CDB_TRY(m_status);
    m_is_setup = true;
    return Status::ok();
}

DBImpl::~DBImpl()
{
    if (m_is_setup && m_status.is_ok()) {
        if (const auto s = wal->flush(); !s.is_ok()) {
            m_info_log->logv("failed to flush wal: %s", s.to_string().data());
        }
        if (const auto s = pager->flush(m_commit_lsn); !s.is_ok()) {
            m_info_log->logv("failed to flush pager: %s", s.to_string().data());
        }
        if (const auto s = wal->close(); !s.is_ok()) {
            m_info_log->logv("failed to close wal: %s", s.to_string().data());
        }
        if (const auto s = ensure_consistency(); !s.is_ok()) {
            m_info_log->logv("failed to ensure consistency: %s", s.to_string().data());
        }
    }

    if (m_owns_info_log) {
        delete m_info_log;
    }
    if (m_owns_env) {
        delete m_env;
    }

    delete pager;
    delete tree;
    delete wal;
}

auto DBImpl::repair(const Options &options, const std::string &filename) -> Status
{
    (void)filename;
    (void)options;
    return Status::logic_error("<NOT IMPLEMENTED>"); // TODO: repair() operation attempts to fix a
                                                     // database that could not be opened due to
                                                     // corruption that couldn't/shouldn't be rolled
                                                     // back.
}

auto DBImpl::destroy(const Options &options, const std::string &filename) -> Status
{
    bool owns_env {};
    Env *env;

    if (options.env) {
        env = options.env;
    } else {
        env = new EnvPosix;
        owns_env = true;
    }

    const auto [dir, base] = split_path(filename);
    const auto path = join_paths(dir, base);
    auto wal_prefix = options.wal_prefix;
    if (wal_prefix.empty()) {
        wal_prefix = path + kDefaultWalSuffix;
    }
    if (options.info_log == nullptr) {
        (void)env->remove_file(path + kDefaultLogSuffix);
    }
    // TODO: Make sure this file is a CalicoDB database.
    auto s = env->remove_file(path);

    std::vector<std::string> children;
    auto t = env->get_children(dir, &children);
    if (s.is_ok()) {
        s = t;
    }
    if (t.is_ok()) {
        for (const auto &name : children) {
            const auto sibling_filename = join_paths(dir, name);
            if (sibling_filename.find(wal_prefix) == 0) {
                t = env->remove_file(sibling_filename);
                if (s.is_ok()) {
                    s = t;
                }
            }
        }
    }

    if (owns_env) {
        delete env;
    }
    return s;
}

auto DBImpl::status() const -> Status
{
    return m_status;
}

auto DBImpl::get_property(const Slice &name, std::string *out) const -> bool
{
    if (Slice prop {name}; prop.starts_with("calicodb.")) {
        prop.advance(std::strlen("calicodb."));

        if (prop == "counts") {
            out->append("records:");
            append_number(*out, m_record_count);
            out->append(",pages:");
            append_number(*out, pager->page_count());
            out->append(",updates:");
            append_number(*out, m_txn_size);
            return true;

        } else if (prop == "stats") {
            out->append("cache_hit_ratio:");
            append_double(*out, pager->hit_ratio());
            out->append(",data_throughput:");
            append_number(*out, m_bytes_written);
            out->append(",pager_throughput:");
            append_number(*out, pager->bytes_written());
            out->append(",wal_throughput:");
            append_number(*out, wal->bytes_written());
            return true;
        }
    }
    return false;
}

auto DBImpl::get(const Slice &key, std::string *value) const -> Status
{
    CDB_TRY(m_status);
    return tree->get(key, value);
}

auto DBImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(*tree);
    if (!m_status.is_ok()) {
        CursorInternal::invalidate(*cursor, m_status);
    }
    return cursor;
}

auto DBImpl::put(const Slice &key, const Slice &value) -> Status
{
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }
    CDB_TRY(m_status);

    bool exists;
    if (auto s = tree->put(key, value, &exists); !s.is_ok()) {
        SET_STATUS(s);
        return s;
    }
    const auto inserted = !exists;
    m_bytes_written += key.size() * inserted + value.size();
    m_record_count += inserted;
    m_txn_size++;
    return Status::ok();
}

auto DBImpl::erase(const Slice &key) -> Status
{
    CDB_TRY(m_status);

    auto s = tree->erase(key);
    if (s.is_ok()) {
        m_record_count--;
        m_txn_size++;
    } else if (!s.is_not_found()) {
        SET_STATUS(s);
    }
    return s;
}

auto DBImpl::vacuum() -> Status
{
    CDB_TRY(m_status);
    if (auto s = do_vacuum(); !s.is_ok()) {
        SET_STATUS(s);
    }
    return m_status;
}

auto DBImpl::do_vacuum() -> Status
{
    Id target {pager->page_count()};
    if (target.is_root()) {
        return Status::ok();
    }
    const auto original = target;
    for (;; target.value--) {
        bool vacuumed;
        CDB_TRY(tree->vacuum_one(target, &vacuumed));
        if (!vacuumed) {
            break;
        }
    }
    if (target.value == pager->page_count()) {
        // No pages available to vacuum: database is minimally sized.
        return Status::ok();
    }
    // Make sure the vacuum updates are in the WAL. If this succeeds, we should
    // be able to reapply the whole vacuum operation if the truncation fails.
    // The recovery routine should truncate the file to match the header page
    // count if necessary.
    CDB_TRY(wal->flush());
    CDB_TRY(pager->truncate(target.value));

    m_info_log->logv("vacuumed %llu pages", original.value - target.value);
    return pager->flush();
}

auto DBImpl::commit() -> Status
{
    CDB_TRY(m_status);
    if (m_txn_size != 0) {
        if (auto s = do_commit(); !s.is_ok()) {
            SET_STATUS(s);
            return s;
        }
    }
    return Status::ok();
}

auto DBImpl::do_commit() -> Status
{
    m_txn_size = 0;

    Page root;
    CDB_TRY(pager->acquire(Id::root(), root));
    pager->upgrade(root);

    // The root page is guaranteed to have a full image in the WAL. The current
    // LSN is now the same as the commit LSN.
    auto commit_lsn = wal->current_lsn();
    m_info_log->logv("commit requested at lsn %llu", commit_lsn.value);

    CDB_TRY(save_state(std::move(root), commit_lsn));
    CDB_TRY(wal->flush());

    m_info_log->logv("commit successful");
    m_commit_lsn = commit_lsn;
    return Status::ok();
}

auto DBImpl::ensure_consistency() -> Status
{
    Recovery recovery {*pager, *wal, m_commit_lsn};

    m_in_txn = false;
    CDB_TRY(recovery.recover());
    m_in_txn = true;
    return load_state();
}

auto DBImpl::save_state(Page root, Lsn commit_lsn) const -> Status
{
    CDB_EXPECT_TRUE(root.id().is_root());
    CDB_EXPECT_FALSE(commit_lsn.is_null());

    FileHeader header;
    header.read(root.data());

    pager->save_state(header);
    header.freelist_head = m_freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    header.commit_lsn = commit_lsn;
    header.record_count = m_record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());
    header.write(root.span(0, FileHeader::kSize).data());
    pager->release(std::move(root));

    return Status::ok();
}

auto DBImpl::load_state() -> Status
{
    Page root;
    CDB_TRY(pager->acquire(Id::root(), root));

    FileHeader header;
    header.read(root.data());
    const auto expected_crc = crc32c::Unmask(header.header_crc);
    const auto computed_crc = header.compute_crc();
    if (expected_crc != computed_crc) {
        m_info_log->logv("file header crc mismatch (expected %u but computed %u)", expected_crc, computed_crc);
        return Status::corruption("crc mismatch");
    }

    m_commit_lsn = header.commit_lsn;
    m_record_count = header.record_count;
    pager->load_state(header);
    tree->load_state(header);

    pager->release(std::move(root));
    return Status::ok();
}

auto DBImpl::TEST_validate() const -> void
{
    tree->TEST_validate();
}

auto setup(const std::string &path, Env &env, const Options &options, FileHeader &header) -> Status
{
    static constexpr std::size_t kMinFrameCount {16};

    if (options.page_size < kMinPageSize) {
        return Status::invalid_argument("page size is too small");
    }

    if (options.page_size > kMaxPageSize) {
        return Status::invalid_argument("page size is too large");
    }

    if (!is_power_of_two(options.page_size)) {
        return Status::invalid_argument("page size is not a power of 2");
    }

    if (options.cache_size < options.page_size * kMinFrameCount) {
        return Status::invalid_argument("page cache is too small");
    }

    std::unique_ptr<Reader> reader;
    Reader *reader_temp;

    if (auto s = env.new_reader(path, &reader_temp); s.is_ok()) {
        reader.reset(reader_temp);
        std::size_t file_size {};
        CDB_TRY(env.file_size(path, &file_size));

        if (file_size < FileHeader::kSize) {
            return Status::invalid_argument("file is not a database");
        }

        char buffer[FileHeader::kSize];
        auto read_size = sizeof(buffer);
        CDB_TRY(reader->read(buffer, &read_size, 0));
        if (read_size != sizeof(buffer)) {
            return Status::system_error("incomplete read of file header");
        }
        header.read(buffer);

        if (header.magic_code != FileHeader::kMagicCode) {
            return Status::invalid_argument("file is not a database");
        }
        if (crc32c::Unmask(header.header_crc) != header.compute_crc()) {
            return Status::corruption("file header is corrupted");
        }
        if (header.page_size == 0) {
            return Status::corruption("header indicates a page size of 0");
        }
        if (file_size % header.page_size) {
            return Status::corruption("database size is invalid");
        }

    } else if (s.is_not_found()) {
        header.page_size = static_cast<std::uint16_t>(options.page_size);
        header.header_crc = header.compute_crc();

    } else {
        return s;
    }

    if (header.page_size < kMinPageSize) {
        return Status::corruption("header page size is too small");
    }
    if (header.page_size > kMaxPageSize) {
        return Status::corruption("header page size is too large");
    }
    if (!is_power_of_two(header.page_size)) {
        return Status::corruption("header page size is not a power of 2");
    }
    return Status::ok();
}

#undef SET_STATUS

} // namespace calicodb

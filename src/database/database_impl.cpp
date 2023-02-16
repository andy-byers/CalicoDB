
#include "database_impl.h"
#include <limits>
#include "calico/calico.h"
#include "calico/storage.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/cursor_internal.h"
#include "utils/logging.h"

namespace Calico {

#define Maybe_Set_Error(expr) \
    do { \
        if (auto maybe_error_s = (expr); m_status.is_ok() && !maybe_error_s.is_ok()) { \
            m_status = maybe_error_s; \
        } \
    } while (0)

[[nodiscard]]
static auto sanitize_options(const Options &options) -> Options
{
    static constexpr Size KiB {1'024};

    const auto page_size = options.page_size;
    auto cache_size = options.cache_size;

    if (options.page_size <= 2 * KiB) {
        cache_size = 2048 * page_size;
    } else if (options.page_size <= 16 * KiB) {
        cache_size = 256 * page_size;
    } else {
        cache_size = 128 * page_size;
    }

    auto sanitized = options;
    if (sanitized.cache_size == 0) {
        sanitized.cache_size = cache_size;
    }
    return sanitized;
}

auto DatabaseImpl::open(const Slice &path, const Options &options) -> Status
{
    auto sanitized = sanitize_options(options);

    m_db_prefix = path.to_string();
    if (m_db_prefix.back() != '/') {
        m_db_prefix += '/';
    }
    m_wal_prefix = sanitized.wal_prefix.to_string();
    if (m_wal_prefix.empty()) {
        m_wal_prefix = m_db_prefix + "wal-";
    }

    // Any error during initialization is fatal.
    return do_open(sanitized);
}

auto DatabaseImpl::do_open(Options sanitized) -> Status
{
    m_storage = sanitized.storage;
    if (m_storage == nullptr) {
        m_storage = new(std::nothrow) PosixStorage;
        if (m_storage == nullptr) {
            return Status::system_error("out of memory");
        }
        m_owns_storage = true;
    }

    (void)m_storage->create_directory(m_db_prefix);

    m_info_log = sanitized.info_log;
    if (m_info_log == nullptr) {
        Calico_Try_S(m_storage->new_logger(m_db_prefix + "log", &m_info_log));
        sanitized.info_log = m_info_log;
        m_owns_info_log = true;
    }

    const auto initial = setup(m_db_prefix, *m_storage, sanitized);
    if (!initial.has_value()) {
        return initial.error();
    }
    auto [state, is_new] = *initial;
    if (!is_new) {
        sanitized.page_size = state.page_size;
    }

    max_key_length = compute_max_local(sanitized.page_size);
    m_scratch.resize(wal_scratch_size(sanitized.page_size));

    {
        auto r = WriteAheadLog::open({
            m_wal_prefix,
            m_storage,
            sanitized.page_size,
            256,
        });
        if (!r.has_value()) {
            return r.error();
        }
        wal = std::move(*r);
    }

    {
        auto r = Pager::open({
            m_db_prefix,
            m_storage,
            &m_scratch,
            wal.get(),
            m_info_log,
            &m_status,
            &m_commit_lsn,
            &m_in_txn,
            sanitized.cache_size / sanitized.page_size,
            sanitized.page_size,
        });
        if (!r.has_value()) {
            return r.error();
        }
        pager = std::move(*r);
        pager->load_state(state);
    }

    tree = std::make_unique<BPlusTree>(*pager);
    tree->load_state(state);
    m_recovery = std::make_unique<Recovery>(*pager, *wal, m_commit_lsn);

    Status s;
    if (is_new) {
        logv(m_info_log, "setting up a new database");
        Calico_Try_S(wal->start_workers());
        auto root = tree->setup();
        if (!root.has_value()) {
            return root.error();
        }
        CALICO_EXPECT_EQ(pager->page_count(), 1);

        state.page_count = 1;
        state.write(root->page);
        state.header_crc = state.compute_crc();
        state.write(root->page);
        pager->release(std::move(*root).take());
        Calico_Try_S(do_commit({}));

    } else {
        logv(m_info_log, "ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        Calico_Try_S(ensure_consistency_on_startup());
    }
    logv(m_info_log, "pager recovery lsn is ", pager->recovery_lsn().value);
    logv(m_info_log, "wal flushed lsn is ", wal->flushed_lsn().value);
    logv(m_info_log, "commit lsn is ", m_commit_lsn.value);
    return status();
}

DatabaseImpl::~DatabaseImpl()
{
    if (m_recovery) {
        if (auto s = wal->close(); !s.is_ok()) {
            logv(m_info_log, "failed to flush wal: %s", s.what().to_string());
        }
        if (auto s = pager->flush({}); !s.is_ok()) {
            logv(m_info_log, "failed to flush pager: %s", s.what().to_string());
        }
        if (auto s = pager->sync(); !s.is_ok()) {
            logv(m_info_log, "failed to sync pager: %s", s.what().to_string());
        }
    }

    if (m_owns_info_log) {
        delete m_info_log;
    }

    if (m_owns_storage) {
        delete m_storage;
    }
}

auto DatabaseImpl::repair(const std::string &path, const Options &options) -> Status
{
    (void)path;(void)options;
    return Status::logic_error("<NOT IMPLEMENTED>"); // TODO: repair() operation attempts to fix a database that could not be opened due to corruption that couldn't/shouldn't be rolled back.
}

auto DatabaseImpl::destroy(const std::string &path, const Options &options) -> Status
{
    bool owns_storage {};
    Storage *storage;

    if (options.storage) {
        storage = options.storage;
    } else {
        storage = new PosixStorage;
        owns_storage = true;
    }

    auto prefix = path;
    if (prefix.back() != '/') {
        prefix += '/';
    }

    std::vector<std::string> children;
    if (auto s = storage->get_children(path, children); s.is_ok()) {
        for (const auto &name: children) {
            (void)storage->remove_file(prefix + name);
        }
    }

    if (!options.wal_prefix.is_empty()) {
        children.clear();

        auto dir_path = options.wal_prefix.to_string();
        if (auto pos = dir_path.rfind('/'); pos != std::string::npos) {
            dir_path.erase(pos + 1);
        }

        if (auto s = storage->get_children(dir_path, children); s.is_ok()) {
            for (const auto &name: children) {
                const auto filename = dir_path + name;
                if (Slice {filename}.starts_with(options.wal_prefix)) {
                    (void)storage->remove_file(filename);
                }
            }
        }
    }
    auto s = storage->remove_directory(path);

    if (owns_storage) {
        delete storage;
    }
    return s;
}

auto DatabaseImpl::status() const -> Status
{
    Maybe_Set_Error(wal->status());
    return m_status;
}

auto DatabaseImpl::get_property(const Slice &name, std::string &out) const -> bool
{
    if (Slice prop {name}; prop.starts_with("calico.")) {
        prop.advance(7);

        if (prop == "counts") {
            out.append("records:");
            append_number(out, record_count);
            out.append(",pages:");
            append_number(out, pager->page_count());
            out.append(",updates:");
            append_number(out, m_txn_size);
            return true;

        } else if (prop == "stats") {
            out.append("cache_hit_ratio:");
            append_double(out, pager->hit_ratio());
            out.append(",data_throughput:");
            append_number(out, bytes_written);
            out.append(",pager_throughput:");
            append_number(out, pager->bytes_written());
            out.append(",wal_throughput:");
            append_number(out, wal->bytes_written());
            return true;
        }
    }
    return false;
}

auto DatabaseImpl::check_key(const Slice &key) const -> Status
{
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }
    if (key.size() > max_key_length) {
        return Status::invalid_argument("key is too long");
    }
    return Status::ok();
}

auto DatabaseImpl::get(const Slice &key, std::string &value) const -> Status
{
    Calico_Try_S(status());
    if (auto slot = tree->search(key)) {
        auto [node, index, exact] = std::move(*slot);

        if (!exact) {
            pager->release(std::move(node.page));
            return Status::not_found("not found");
        }

        if (auto result = tree->collect(std::move(node), index)) {
            value = std::move(*result);
            return Status::ok();
        } else {
            return result.error();
        }
    } else {
        return slot.error();
    }
}

auto DatabaseImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(*tree);
    if (auto s = status(); cursor && !s.is_ok()) {
        CursorInternal::invalidate(*cursor, s);
    }
    return cursor;
}

auto DatabaseImpl::put(const Slice &key, const Slice &value) -> Status
{
    Calico_Try_S(status());
    Calico_Try_S(check_key(key));

    // Value is greater than 4 GiB in length.
    if (value.size() > std::numeric_limits<ValueSize>::max()) {
        return Status::invalid_argument("cannot insert record: value is too long");
    }

    bytes_written += key.size() + value.size();
    if (const auto inserted = tree->insert(key, value)) {
        record_count += *inserted;
        m_txn_size++;
        return Status::ok();
    } else {
        Maybe_Set_Error(inserted.error());
        return inserted.error();
    }
}

auto DatabaseImpl::erase(const Slice &key) -> Status
{
    Calico_Try_S(status());
    Calico_Try_S(check_key(key));
    if (const auto erased = tree->erase(key)) {
        record_count--;
        m_txn_size++;
        return Status::ok();
    } else {
        if (!erased.error().is_not_found()) {
            Maybe_Set_Error(erased.error());
        }
        return erased.error();
    }
}

auto DatabaseImpl::vacuum() -> Status
{
    Calico_Try_S(status());
    if (m_txn_size) {
        return Status::logic_error("transaction must be empty");
    }
    Maybe_Set_Error(do_vacuum());
    return status();
}

auto DatabaseImpl::do_vacuum() -> Status
{
    Id target {pager->page_count()};
    if (target.is_root()) {
        return Status::ok();
    }
    for (; ; target.value--) {
        if (auto r = tree->vacuum_one(target)) {
            if (!*r) {
                break;
            }
        } else {
            return r.error();
        }
    }
    if (target.value == pager->page_count()) {
        return Status::ok();
    }
    // Make sure the vacuum updates are in the WAL. If this succeeds, we should be able to reapply the
    // whole vacuum operation if the truncation fails. The recovery routine should truncate the file
    // to match the header if necessary.
    pager->m_frames.m_page_count = target.value;
    Calico_Try_S(do_commit(m_commit_lsn));
    if (auto r = pager->truncate(pager->page_count())) {
        // The file size now matches the header page count.
    } else {
        return r.error();
    }
    return do_commit({});
}

auto DatabaseImpl::commit() -> Status
{
    Calico_Try_S(status());
    if (m_txn_size != 0) {
        return do_commit(m_commit_lsn);
    }
    return Status::ok();
}

/*
 * NOTE: This method only returns an error status if the commit record could not be flushed to the WAL, since this
 *       is what ultimately determines the transaction outcome. If a different failure occurs, that status will be
 *       returned on the next access to the database object.
 */
auto DatabaseImpl::do_commit(Lsn flush_lsn) -> Status
{
    logv(m_info_log, "commit requested at lsn ", wal->current_lsn().value + 1);

    m_txn_size = 0;
    Calico_Try_S(save_state());

    const auto lsn = wal->current_lsn();
    wal->log(encode_commit_payload(lsn, m_scratch));
    Calico_Try_S(wal->flush());
    wal->advance();

    Maybe_Set_Error(pager->flush(flush_lsn));
    wal->cleanup(pager->recovery_lsn());
    m_commit_lsn = lsn;

    logv(m_info_log, "commit successful");
    return Status::ok();
}

auto DatabaseImpl::ensure_consistency_on_startup() -> Status
{
    m_in_txn = false;
    Calico_Try_S(m_recovery->start_recovery());
    Calico_Try_S(load_state());
    Calico_Try_S(m_recovery->finish_recovery());
    m_in_txn = true;
    return Status::ok();
}

auto DatabaseImpl::save_state() const -> Status
{
    auto root = pager->acquire(Id::root());
    if (!root.has_value()) {
        return root.error();
    }
    pager->upgrade(*root);
    FileHeader header {*root};
    pager->save_state(header);
    tree->save_state(header);
    header.record_count = record_count;
    header.header_crc = header.compute_crc();
    header.write(*root);

    pager->release(std::move(*root));
    return Status::ok();
}

auto DatabaseImpl::load_state() -> Status
{
    auto root = pager->acquire(Id::root());
    if (!root.has_value()) {
        return root.error();
    }

    FileHeader header {*root};
    if (header.header_crc != header.compute_crc()) {
        auto s = Status::corruption("file header is corrupted");
        logv(m_info_log, "cannot load database state: ", s.what().to_string());
        return s;
    }

    const auto before_count = pager->page_count();

    record_count = header.record_count;
    pager->load_state(header);
    tree->load_state(header);

    pager->release(std::move(*root));
    if (pager->page_count() < before_count) {
        const auto after_size = pager->page_count() * pager->page_size();
        return m_storage->resize_file(m_db_prefix + "data", after_size);
    }
    return Status::ok();
}

auto DatabaseImpl::TEST_validate() const -> void
{
    tree->TEST_check_links();
    tree->TEST_check_order();
    tree->TEST_check_nodes();
}

auto setup(const std::string &prefix, Storage &store, const Options &options) -> tl::expected<InitialState, Status>
{
    static constexpr Size MINIMUM_BUFFER_COUNT {16};
    FileHeader header;

    if (options.page_size < MINIMUM_PAGE_SIZE) {
        return tl::make_unexpected(Status::invalid_argument("page size is too small"));
    }

    if (options.page_size > MAXIMUM_PAGE_SIZE) {
        return tl::make_unexpected(Status::invalid_argument("page size is too large"));
    }

    if (!is_power_of_two(options.page_size)) {
        return tl::make_unexpected(Status::invalid_argument("page size is not a power of 2"));
    }

    if (options.cache_size < options.page_size * MINIMUM_BUFFER_COUNT) {
        return tl::make_unexpected(Status::invalid_argument("page cache is too small"));
    }

    if (auto s = store.create_directory(prefix); !s.is_ok() && !s.is_logic_error()) {
        return tl::make_unexpected(s);
    }

    const auto path = prefix + "data";
    std::unique_ptr<Reader> reader;
    Reader *reader_temp {};
    bool exists {};

    if (auto s = store.new_reader(path, &reader_temp); s.is_ok()) {
        reader.reset(reader_temp);
        Size file_size {};
        s = store.file_size(path, file_size);
        if (!s.is_ok()) {
            return tl::make_unexpected(s);
        }
        if (file_size < FileHeader::SIZE) {
            return tl::make_unexpected(Status::corruption("database is smaller than file header"));
        }

        Byte buffer[FileHeader::SIZE];
        Span span {buffer, sizeof(buffer)};
        s = read_exact_at(*reader, span, 0);

        header = FileHeader {
            Page {Id::root(), span, false}
        };

        if (!s.is_ok()) {
            return tl::make_unexpected(s);
        }
        if (header.page_size == 0) {
            return tl::make_unexpected(Status::corruption("header indicates a page size of 0"));
        }
        if (file_size % header.page_size) {
            return tl::make_unexpected(Status::corruption("database size is invalid"));
        }
        if (header.magic_code != FileHeader::MAGIC_CODE) {
            return tl::make_unexpected(Status::invalid_argument("magic code is invalid"));
        }
        if (header.header_crc != header.compute_crc()) {
            return tl::make_unexpected(Status::corruption("file header is corrupted"));
        }
        exists = true;

    } else if (s.is_not_found()) {
        header.page_size = static_cast<std::uint16_t>(options.page_size);
        header.recovery_lsn = Id::root();
        header.header_crc = header.compute_crc();

    } else {
        return tl::make_unexpected(s);
    }

    if (header.page_size < MINIMUM_PAGE_SIZE) {
        return tl::make_unexpected(Status::corruption("header page size is too small"));
    }
    if (header.page_size > MAXIMUM_PAGE_SIZE) {
        return tl::make_unexpected(Status::corruption("header page size is too large"));
    }
    if (!is_power_of_two(header.page_size)) {
        return tl::make_unexpected(Status::corruption("header page size is not a power of 2"));
    }
    return InitialState {header, !exists};
}

#undef Maybe_Set_Error

} // namespace Calico

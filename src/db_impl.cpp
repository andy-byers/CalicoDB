
#include "db_impl.h"
#include "calicodb/calicodb.h"
#include "calicodb/env.h"
#include "crc.h"
#include "env_posix.h"
#include "logging.h"
#include "table_impl.h"
#include "wal_reader.h"

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

[[nodiscard]] static auto encode_logical_id(LogicalPageId id) -> std::string
{
    std::string out;
    append_number(&out, id.table_id.value);
    out += ',';
    append_number(&out, id.page_id.value);
    return out;
}

[[nodiscard]] static auto decode_logical_id(const Slice &in, LogicalPageId *out) -> Status
{
    Slice value {in};
    if (!consume_decimal_number(&value, &out->table_id.value)) {
        return Status::corruption("table id is corrupted");
    }
    if (value[0] != ',') {
        return Status::corruption("logical id is missing separator");
    }
    value.advance();
    if (!consume_decimal_number(&value, &out->page_id.value)) {
        return Status::corruption("page id is corrupted");
    }
    return Status::ok();
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

    bool db_exists {};
    if (auto s = m_env->file_exists(m_filename); s.is_not_found()) {
        if (!sanitized.create_if_missing) {
            return Status::invalid_argument("database does not exist");
        }
    } else if (s.is_ok()) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument("database already exists");
        }
        db_exists = true;
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
    m_commit_lsn = state.last_table_id;
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
            &m_is_running,
            sanitized.cache_size / sanitized.page_size,
            sanitized.page_size,
        },
        &pager));

    if (!db_exists) {
        m_info_log->logv("setting up a new database");

        // Create the root tree.
        CDB_TRY(Tree::create(*pager, Id::root(), m_freelist_head));
        m_last_table_id = Id::root();

        // Write the initial file header.
        Page page {LogicalPageId::unknown_table(Id::root())};
        CDB_TRY(pager->acquire(page));
        pager->upgrade(page);
        state.write(page.span(0, FileHeader::kSize).data());
        pager->release(std::move(page));
        CDB_TRY(pager->flush());
    }
    pager->load_state(state);

    // Open the root table.
    auto *root = new Tree {*pager, LogicalPageId::root(), m_freelist_head};
    m_tables.emplace(Id::root(), TableState {root, Lsn::null()});
    m_root = &m_tables[Id::root()];

    tree = m_root->tree; // TODO: remove

    if (db_exists) {
        m_info_log->logv("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CDB_TRY(ensure_consistency());
        CDB_TRY(load_state());
    }
    CDB_TRY(wal->start_writing());

    m_info_log->logv("pager recovery lsn is %llu", pager->recovery_lsn().value);
    m_info_log->logv("wal flushed lsn is %llu", wal->flushed_lsn().value);
    m_info_log->logv("commit lsn is %llu", m_commit_lsn.value);

    CDB_TRY(m_status);
    m_is_running = true;

    // TODO: Remove eventually.
    auto root_id = LogicalPageId::unknown();
    if (!db_exists){
        CDB_TRY(create_table("temp_table", &root_id));
    }
    CDB_TRY(new_table({}, "temp_table", &m_temp));
    return Status::ok();
}

DBImpl::~DBImpl()
{
    if (m_is_running && m_status.is_ok()) {
        if (const auto s = wal->flush(); !s.is_ok()) {
            m_info_log->logv("failed to flush wal: %s", s.to_string().data());
        }
        if (const auto s = pager->flush(m_commit_lsn); !s.is_ok()) {
            m_info_log->logv("failed to flush pager: %s", s.to_string().data());
        }
        if (const auto s = wal->close(); !s.is_ok()) {
            m_info_log->logv("failed to close wal: %s", s.to_string().data());
        }
        m_is_running = false;
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

    delete m_temp; // TODO: remove
}

auto DBImpl::record_count() const -> std::size_t
{
    return m_record_count;
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
            append_number(out, m_record_count);
            out->append(",pages:");
            append_number(out, pager->page_count());
            out->append(",updates:");
            append_number(out, m_txn_size);
            return true;

        } else if (prop == "stats") {
            out->append("cache_hit_ratio:");
            append_double(out, pager->hit_ratio());
            out->append(",data_throughput:");
            append_number(out, m_bytes_written);
            out->append(",pager_throughput:");
            append_number(out, pager->bytes_written());
            out->append(",wal_throughput:");
            append_number(out, wal->bytes_written());
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

    Page root {LogicalPageId::root()};
    CDB_TRY(pager->acquire(root));
    pager->upgrade(root);

    // The root page is guaranteed to have a full image in the WAL. The current
    // LSN is now the same as the commit LSN.
    auto commit_lsn = wal->current_lsn();
    m_info_log->logv("commit requested at lsn %llu", commit_lsn.value);

    save_state(root, commit_lsn);
    pager->release(std::move(root));
    CDB_TRY(wal->flush());

    m_info_log->logv("commit successful");
    m_commit_lsn = commit_lsn;
    return Status::ok();
}

auto DBImpl::ensure_consistency() -> Status
{
    Recovery recovery {*pager, *wal, m_commit_lsn};
    CDB_TRY(recovery.recover());
    return load_state();
}

auto DBImpl::save_state(Page &root, Lsn commit_lsn) const -> void
{
    CDB_EXPECT_TRUE(root.id().page_id.is_root());
    CDB_EXPECT_FALSE(commit_lsn.is_null());

    FileHeader header;
    header.read(root.data());

    pager->save_state(header);
    header.freelist_head = m_freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    header.last_table_id = m_last_table_id;
    header.commit_lsn = m_commit_lsn; // TODO: remove
    header.record_count = m_record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());
    header.write(root.span(0, FileHeader::kSize).data());
}

auto DBImpl::load_state() -> Status
{
    Page root {LogicalPageId::root()};
    CDB_TRY(pager->acquire(root));

    FileHeader header;
    header.read(root.data());
    const auto expected_crc = crc32c::Unmask(header.header_crc);
    const auto computed_crc = header.compute_crc();
    if (expected_crc != computed_crc) {
        m_info_log->logv("file header crc mismatch (expected %u but computed %u)", expected_crc, computed_crc);
        return Status::corruption("crc mismatch");
    }

    m_commit_lsn = header.commit_lsn; // TODO: remove
    m_last_table_id = header.last_table_id;
    m_record_count = header.record_count;
    m_freelist_head = header.freelist_head;
    pager->load_state(header);

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
        header.page_count = 1;
        header.page_size = static_cast<std::uint16_t>(options.page_size);
        header.header_crc = crc32c::Mask(header.compute_crc());

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

auto DBImpl::new_table(const TableOptions &, const Slice &name, Table **out) -> Status
{
    auto root_id = LogicalPageId::unknown();
    std::string value;

    auto s = m_root->tree->get(name, &value);
    if (s.is_ok()) {
        CDB_TRY(decode_logical_id(value, &root_id));
    } else if (s.is_not_found()) {
        s = create_table(name, &root_id);
    } else {
        SET_STATUS(s);
        return s;
    }

    TableState *state;
    s = open_table(root_id, &state);
    if (s.is_ok()) {
        *out = new TableImpl {root_id.table_id, *this, *state, m_status};
    } else {
        SET_STATUS(s);
    }
    return s;
}

auto DBImpl::create_table(const Slice &name, LogicalPageId *root_id) -> Status
{
    ++m_last_table_id.value;
    root_id->table_id = m_last_table_id;
    CDB_TRY(Tree::create(*pager, m_last_table_id, m_freelist_head, &root_id->page_id));
    CDB_TRY(tree->put(name, encode_logical_id(*root_id)));
    return commit_table(LogicalPageId::root(), *m_root);
}

auto DBImpl::open_table(const LogicalPageId &root_id, TableState **out) -> Status
{
    auto itr = m_tables.find(root_id.table_id);
    if (itr != end(m_tables)) {
        return Status::logic_error("table is already open");
    }

    Page page {root_id};
    CDB_TRY(pager->acquire(page));
    const Lsn commit_lsn {get_u64(page.data() + page_offset(page) + kPageHeaderSize)};
    pager->release(std::move(page));

    m_root_map.insert({root_id.page_id, root_id.table_id});
    itr = m_tables.insert(itr, {root_id.table_id, {}});
    itr->second.tree = new Tree {*pager, root_id, m_freelist_head};
    itr->second.commit_lsn = commit_lsn;
    *out = &itr->second;
    return Status::ok();
}

auto DBImpl::commit_table(const LogicalPageId &root_id, TableState &state) -> Status
{
    Page page {root_id};
    CDB_TRY(pager->acquire(page));
    pager->upgrade(page);

    // The root page is guaranteed to have a full image in the WAL. The current
    // LSN is now the same as the commit LSN.
    state.commit_lsn = wal->current_lsn();

    auto tree_header = page.span(page_offset(page) + kPageHeaderSize, kTreeHeaderSize);
    put_u64(tree_header.data(), state.commit_lsn.value);
    if (root_id.table_id.is_root()) {
        CDB_EXPECT_TRUE(root_id.page_id.is_root());
        save_state(page, state.commit_lsn);
    }

    write_page_lsn(page, wal->current_lsn());
    const auto deltas = page.deltas();
    CDB_EXPECT_EQ(deltas.size(), 1);

    CDB_TRY(wal->log_commit(
        root_id, page.view(0), deltas.front(), nullptr));

    pager->discard(std::move(page));
    return wal->flush();
}

auto DBImpl::close_table(const LogicalPageId &root_id) -> void
{
    const auto itr = m_tables.find(root_id.table_id);
    if (itr == end(m_tables)) {
        return;
    }
//    Recovery recovery {*pager, *wal, m_commit_lsn};
//    recovery();
    // TODO: Make this table consistent.
    delete itr->second.tree;
    m_root_map.erase(root_id.page_id);
    m_tables.erase(itr);
}
//
//auto DBImpl::find_checkpoints(std::unordered_map<Id, Lsn, Id::Hash> *checkpoints) -> Status
//{
//    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*m_root->tree)};
//    cursor->seek_first();
//
//    while (cursor->is_valid()) {
//        auto logical_id = LogicalPageId::unknown_page(Id::root());
//        CDB_TRY(decode_logical_id(cursor->value(), &logical_id));
//        auto itr = checkpoints->find(logical_id.page_id);
//        if (itr != end(*checkpoints)) {
//            return Status::corruption("encountered duplicate root");
//        }
//
//        Page page {logical_id};
//        CDB_TRY(pager->acquire(page));
//        const Lsn commit_lsn {get_u64(page.data() + page_offset(page) + kPageHeaderSize)};
//        checkpoints->insert(itr, {logical_id.table_id, commit_lsn});
//        cursor->next();
//    }
//}
//
//static auto apply_undo(Page &page, const ImageDescriptor &image)
//{
//    const auto data = image.image;
//    mem_copy(page.span(0, data.size()), data);
//    if (page.size() > data.size()) {
//        mem_clear(page.span(data.size(), page.size() - data.size()));
//    }
//}
//
//static auto apply_redo(Page &page, const DeltaDescriptor &deltas)
//{
//    for (auto [offset, data] : deltas.deltas) {
//        mem_copy(page.span(offset, data.size()), data);
//    }
//}
//
//static auto is_commit(const DeltaDescriptor &deltas)
//{
//    return deltas.page_id.is_root() && deltas.deltas.size() == 1 && deltas.deltas.front().offset == 0 &&
//           deltas.deltas.front().data.size() == FileHeader::kSize + sizeof(Lsn);
//}
//
//template <class Descriptor, class Callback>
//static auto with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
//{
//    Page page {LogicalPageId {descriptor.table_id, descriptor.page_id}};
//    CDB_TRY(pager.acquire(page));
//
//    callback(page);
//    pager.release(std::move(page));
//    return Status::ok();
//}
//
//auto DBImpl::recovery_phase_1(std::unordered_map<Id, LogRange, Id::Hash> ranges) -> Status
//{
//    auto &set = wal->m_set;
//
//    if (set.is_empty()) {
//        return Status::ok();
//    }
//
//    std::unique_ptr<Reader> file;
//    auto segment = set.first();
//    Lsn last_lsn;
//
//    const auto translate_status = [&segment, &set](auto s) {
//        CDB_EXPECT_FALSE(s.is_ok());
//        if (s.is_corruption()) {
//            // Allow corruption/incomplete records on the last segment.
//            if (segment == set.last()) {
//                return Status::ok();
//            }
//        }
//        return s;
//    };
//
//    const auto redo = [&](const auto &payload) {
//        const auto decoded = decode_payload(payload);
//        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
//            const auto deltas = std::get<DeltaDescriptor>(decoded);
//            auto itr = ranges.find(deltas.table_id);
//            if (itr == end(ranges)) {
//                // We are not recovering this table right now.
//                return Status::ok();
//            }
//            if (is_commit(deltas)) {
//                itr->second.commit_lsn = deltas.lsn;
//            } else {
//                itr->second.recent_lsn = deltas.lsn;
//            }
//            // WARNING: Applying these updates can cause the in-memory file header variables to be incorrect. This
//            // must be fixed by the caller after this method returns.
//            return with_page(*m_pager, deltas, [this, &deltas](auto &page) {
//                if (read_page_lsn(page) < deltas.lsn) {
//                    m_pager->upgrade(page);
//                    apply_redo(page, deltas);
//                }
//            });
//        } else if (std::holds_alternative<std::monostate>(decoded)) {
//            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
//            return Status::not_found("finished");
//        }
//        return Status::ok();
//    };
//
//    const auto undo = [&](const auto &payload) {
//        const auto decoded = decode_payload(payload);
//        if (std::holds_alternative<ImageDescriptor>(decoded)) {
//            const auto image = std::get<ImageDescriptor>(decoded);
//            if (const auto itr = ranges.find(image.table_id); itr == end(ranges)) {
//                return Status::ok();
//            }
//            return with_page(*pager, image, [this, &image](auto &page) {
//                if (image.lsn < *m_commit_lsn) {
//                    return;
//                }
//                const auto page_lsn = read_page_lsn(page);
//                if (page_lsn.is_null() || page_lsn > image.lsn) {
//                    pager->upgrade(page);
//                    apply_undo(page, image);
//                }
//            });
//        } else if (std::holds_alternative<std::monostate>(decoded)) {
//            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
//            return Status::not_found("finished");
//        }
//        return Status::ok();
//    };
//
//    const auto roll = [&](const auto &action) {
//        CDB_TRY(open_wal_reader(segment, &file));
//        WalReader reader {*file, m_reader_tail};
//
//        for (;;) {
//            Span buffer {m_reader_data};
//            auto s = reader.read(buffer);
//
//            if (s.is_not_found()) {
//                break;
//            } else if (!s.is_ok()) {
//                CDB_TRY(translate_status(s, last_lsn));
//                return Status::ok();
//            }
//
//            last_lsn = extract_payload_lsn(buffer);
//
//            s = action(buffer);
//            if (s.is_not_found()) {
//                break;
//            } else if (!s.is_ok()) {
//                return s;
//            }
//        }
//        return Status::ok();
//    };
//
//    std::unordered_map<Id, Lsn, Id::Hash> checkpoints;
//    CDB_TRY(find_checkpoints(&checkpoints));
//
//    // If ranges is empty, run recovery for all tables.
//    if (ranges.empty()) {
//        for (const auto &checkpoint: checkpoints) {
//            ranges.insert({checkpoint.first, {}});
//        }
//    }
//
//    /* Roll forward, applying missing updates until we reach the end. The final
//     * segment may contain a partial/corrupted record.
//     */
//    for (;; segment = set.id_after(segment)) {
//        CDB_TRY(roll(redo));
//        if (segment == set.last()) {
//            break;
//        }
//    }
//
//    // Didn't make it to the end of the WAL.
//    if (segment != set.last()) {
//        return Status::corruption("wal could not be read to the end");
//    }
//
//    if (last_lsn == last_table_id) {
//        if (*m_commit_lsn <= last_table_id) {
//            *m_commit_lsn = last_table_id;
//            return Status::ok();
//        } else {
//            return Status::corruption("missing commit record");
//        }
//    }
//    *m_commit_lsn = last_table_id;
//
//    /* Roll backward, reverting updates until we reach the most-recent commit. We
//     * are able to read the log forward, since the full images are disjoint.
//     * Again, the last segment we read may contain a partial/corrupted record.
//     */
//    segment = commit_segment;
//    for (; !segment.is_null(); segment = set.id_after(segment)) {
//        CDB_TRY(roll(undo));
//    }
//    return Status::ok();
//}
//
//auto DBImpl::recovery_phase_2(Lsn recent_lsn) -> Status
//{
//    auto &set = wal->m_set;
//
//    // Pager needs the updated state to determine the page count.
//    Page page;
//    CDB_TRY(pager->acquire(Id::root(), page));
//    FileHeader header;
//    header.read(page.data());
//    pager->load_state(header);
//    pager->release(std::move(page));
//
//    // TODO: This is too expensive for large databases. Look into a WAL index?
//    // Make sure we aren't missing any WAL records.
//    // for (auto id = Id::root(); id.value <= pager->page_count(); page_id.value++)
//    // {
//    //    Calico_Try(pager->acquire(Id::root(), page));
//    //    const auto lsn = read_page_lsn(page);
//    //    pager->release(std::move(page));
//    //
//    //    if (lsn > *m_commit_lsn) {
//    //        return Status::corruption("missing wal updates");
//    //    }
//    //}
//
//    /* Make sure all changes have made it to disk, then remove WAL segments from
//     * the right.
//     */
//    CDB_TRY(pager->flush());
//    for (auto id = set.last(); !id.is_null(); id = set.id_before(page_id)) {
//        CDB_TRY(m_env->remove_file(encode_segment_name(wal->m_prefix, page_id)));
//    }
//    set.remove_after(Id::null());
//
//    wal->m_last_lsn = recent_lsn;
//    wal->m_flushed_lsn = recent_lsn;
//    pager->m_recovery_lsn = recent_lsn;
//
//    // Make sure the file size matches the header page count, which should be
//    // correct if we made it this far.
//    CDB_TRY(pager->truncate(pager->page_count()));
//    return pager->sync();
//}
//
//auto DBImpl::open_wal_reader(Id segment, std::unique_ptr<Reader> *out) -> Status
//{
//    Reader *file;
//    const auto name = encode_segment_name(m_wal_prefix, segment);
//    CDB_TRY(m_env->new_reader(name, &file));
//    out->reset(file);
//    return Status::ok();
//}

#undef SET_STATUS

} // namespace calicodb

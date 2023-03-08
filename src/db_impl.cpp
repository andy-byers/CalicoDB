
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

auto TableSet::begin() const -> Iterator
{
    return m_tables.begin();
}

auto TableSet::end() const -> Iterator
{
    return m_tables.end();
}

auto TableSet::get(Id table_id) const -> const TableState *
{
    auto itr = m_tables.find(table_id);
    if (itr == m_tables.end()) {
        return nullptr;
    }
    return &itr->second;
}

auto TableSet::get(Id table_id) -> TableState *
{
    auto itr = m_tables.find(table_id);
    if (itr == m_tables.end()) {
        return nullptr;
    }
    return &itr->second;
}

auto TableSet::add(const LogicalPageId &root_id) -> void
{
    CDB_EXPECT_EQ(get(root_id.table_id), nullptr);
    m_tables.emplace(root_id.table_id, TableState {root_id, Lsn::null(), nullptr});
}

auto TableSet::erase(Id table_id) -> void
{
    m_tables.erase(table_id);
}

#define SET_STATUS(s)           \
    do {                        \
        if (m_status.is_ok()) { \
            m_status = s;       \
        }                       \
    } while (0)

static auto encode_logical_id(LogicalPageId id, char *out) -> void
{
    put_u64(out, id.table_id.value);
    put_u64(out + sizeof(Id), id.page_id.value);
}

[[nodiscard]] static auto decode_logical_id(const Slice &in, LogicalPageId *out) -> Status
{
    if (in.size() != LogicalPageId::kSize) {
        return Status::corruption("logical id is corrupted");
    }
    out->table_id.value = get_u64(in.data());
    out->page_id.value = get_u64(in.data() + sizeof(Id));
    return Status::ok();
}

[[nodiscard]] static auto read_checkpoint_lsn(const Page &page) -> Lsn
{
    return {get_u64(page.data() + page_offset(page) + kPageHeaderSize)};
}

static auto write_checkpoint_lsn(Page &page, Lsn lsn) -> void
{
    put_u64(page.span(page_offset(page) + kPageHeaderSize, kTreeHeaderSize).data(), lsn.value);
}

auto DBImpl::open(const Options &sanitized) -> Status
{
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

    FileHeader state;
    CDB_TRY(setup(m_filename, *m_env, sanitized, &state));
    const auto page_size = state.page_size;

    CDB_TRY(WriteAheadLog::open(
        {
            m_wal_prefix,
            m_env,
            page_size,
        },
        &wal));

    CDB_TRY(Pager::open(
        {
            m_filename,
            m_env,
            wal,
            m_info_log,
            &m_tables,
            &m_status,
            &m_is_running,
            sanitized.cache_size / page_size,
            page_size,
        },
        &pager));

    if (!db_exists) {
        m_info_log->logv("setting up a new database");

        // Create the root tree.
        CDB_TRY(Tree::create(*pager, Id::root(), m_freelist_head));

        // Write the initial file header.
        Page db_root {LogicalPageId::unknown_table(Id::root())};
        CDB_TRY(pager->acquire(db_root));
        pager->upgrade(db_root);
        state.write(db_root.span(0, FileHeader::kSize).data());
        pager->release(std::move(db_root));
        CDB_TRY(pager->flush());
    }
    pager->load_state(state);

    // Open the root table manually.
    m_tables.add(LogicalPageId::root());
    auto *root_state = m_tables.get(Id::root());
    Page db_root {LogicalPageId::root()};
    CDB_TRY(pager->acquire(db_root));
    root_state->tree = new Tree {*pager, &root_state->root_id, m_freelist_head};
    root_state->is_open = true;
    root_state->checkpoint_lsn = read_checkpoint_lsn(db_root);
    pager->release(std::move(db_root));
    m_root = m_tables.get(Id::root());
    CDB_EXPECT_NE(m_root, nullptr);

    auto *cursor = CursorInternal::make_cursor(*m_root->tree);
    while (cursor->is_valid()) {
        auto root_id = LogicalPageId::unknown();
        CDB_TRY(decode_logical_id(cursor->value(), &root_id));
        m_tables.add(root_id);
        cursor->next();
    }
    delete cursor;

    if (db_exists) {
        m_info_log->logv("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        IdMap<LogRange> checkpoints;
        CDB_TRY(find_checkpoints(&checkpoints));
        CDB_TRY(ensure_consistency(checkpoints));
        CDB_TRY(load_state());
    }
    CDB_TRY(wal->start_writing());

    m_info_log->logv("pager recovery lsn is %llu", pager->recovery_lsn().value);
    m_info_log->logv("wal flushed lsn is %llu", wal->flushed_lsn().value);

    CDB_TRY(m_status);
    m_is_running = true;
    return Status::ok();
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_reader_data(wal_scratch_size(options.page_size), '\0'),
      m_reader_tail(wal_block_size(options.page_size), '\0'),
      m_filename {std::move(filename)},
      m_wal_prefix {sanitized.wal_prefix},
      m_env {sanitized.env},
      m_info_log {sanitized.info_log},
      m_last_table_id {Id::root()},
      m_owns_env {options.env == nullptr},
      m_owns_info_log {options.info_log == nullptr}
{}

DBImpl::~DBImpl()
{
    if (m_is_running && m_status.is_ok()) {
        if (const auto s = wal->flush(); !s.is_ok()) {
            m_info_log->logv("failed to flush wal: %s", s.to_string().data());
        }
        if (const auto s = pager->flush(); !s.is_ok()) {
            m_info_log->logv("failed to flush pager: %s", s.to_string().data());
        }
        if (const auto s = wal->close(); !s.is_ok()) {
            m_info_log->logv("failed to erase wal: %s", s.to_string().data());
        }
        m_is_running = false;
        IdMap<LogRange> ranges;
        if (const auto s = find_checkpoints(&ranges); !s.is_ok()) {
            m_info_log->logv("failed to determine table checkpoints: %s", s.to_string().data());
        }
        if (const auto s = ensure_consistency(ranges); !s.is_ok()) {
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
    delete wal;
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
//        bool vacuumed;
        CDB_EXPECT_TRUE(false); // TODO
//        CDB_TRY(tree->vacuum_one(target, &vacuumed));
//        if (!vacuumed) {
//            break;
//        }
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
//
//auto DBImpl::commit() -> Status
//{
//    CDB_TRY(m_status);
//    if (m_txn_size != 0) {
//        if (auto s = do_commit(); !s.is_ok()) {
//            SET_STATUS(s);
//            return s;
//        }
//    }
//    return Status::ok();
//}
//
//auto DBImpl::do_commit() -> Status
//{
//    m_txn_size = 0;
//
//    Page root {LogicalPageId::root()};
//    CDB_TRY(pager->acquire(root));
//    pager->upgrade(root);
//
//    // The root page is guaranteed to have a full image in the WAL. The current
//    // LSN is now the same as the commit LSN.
//    auto checkpoint_lsn = wal->current_lsn();
//    m_info_log->logv("commit requested at lsn %llu", checkpoint_lsn.value);
//
//    finish_checkpoint(root, checkpoint_lsn);
//    pager->release(std::move(root));
//    CDB_TRY(wal->flush());
//
//    m_info_log->logv("commit successful");
//    m_commit_lsn = checkpoint_lsn;
//    return Status::ok();
//}

auto DBImpl::ensure_consistency(IdMap<LogRange> ranges) -> Status
{
    Lsn final_lsn;
    m_is_running = false;
    CDB_TRY(recovery_phase_1(std::move(ranges), &final_lsn));
    CDB_TRY(recovery_phase_2(final_lsn));
    m_is_running = true;
    return load_state();
}

auto DBImpl::finish_checkpoint() -> Status
{
    FileHeader header;
    Page db_root {LogicalPageId::root()};
    CDB_TRY(pager->acquire(db_root));
    pager->upgrade(db_root);

    header.read(db_root.data());
    pager->save_state(header);
    header.freelist_head = m_freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    header.last_table_id = m_last_table_id;
    header.record_count = m_record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());

    auto span = db_root.span(0, FileHeader::kSize);
    header.write(span.data());
    m_root->checkpoint_lsn = wal->current_lsn();
    write_checkpoint_lsn(db_root, m_root->checkpoint_lsn);
    pager->release(std::move(db_root));
    return Status::ok();
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

    m_last_table_id = header.last_table_id;
    m_record_count = header.record_count;
    m_freelist_head = header.freelist_head;
    pager->load_state(header);

    pager->release(std::move(root));
    return Status::ok();
}

auto DBImpl::TEST_tables() const -> const TableSet &
{
    return m_tables;
}

auto DBImpl::TEST_validate() const -> void
{
//    for (const auto &[table_id, state] : m_tables) {
//        state.tree->TEST_validate();
//    }
}

auto setup(const std::string &path, Env &env, const Options &options, FileHeader *header) -> Status
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
        header->read(buffer);

        if (header->magic_code != FileHeader::kMagicCode) {
            return Status::invalid_argument("file is not a database");
        }
        if (crc32c::Unmask(header->header_crc) != header->compute_crc()) {
            return Status::corruption("file header is corrupted");
        }
        if (header->page_size == 0) {
            return Status::corruption("header indicates a page size of 0");
        }
        if (file_size % header->page_size) {
            return Status::corruption("database size is invalid");
        }

    } else if (s.is_not_found()) {
        header->page_count = 1;
        header->page_size = static_cast<std::uint16_t>(options.page_size);
        header->header_crc = crc32c::Mask(header->compute_crc());

    } else {
        return s;
    }

    if (header->page_size < kMinPageSize) {
        return Status::corruption("header page size is too small");
    }
    if (header->page_size > kMaxPageSize) {
        return Status::corruption("header page size is too large");
    }
    if (!is_power_of_two(header->page_size)) {
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
    }

    if (!s.is_ok()) {
        SET_STATUS(s);
        return s;
    }

    auto state = m_tables.get(root_id.table_id);
    CDB_EXPECT_NE(state, nullptr);

    if (state->is_open) {
        return Status::invalid_argument("table is already open");
    }
    s = open_table(*state);
    if (s.is_ok()) {
        *out = new TableImpl {*this, *state, m_status};
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

    char payload[LogicalPageId::kSize];
    encode_logical_id(*root_id, payload);

    // Write an entry for the new table in the root table.
    CDB_TRY(m_root->tree->put(name, Slice {payload, LogicalPageId::kSize}));
    CDB_TRY(checkpoint_table(LogicalPageId::root(), *m_root));

    m_tables.add(*root_id);
    return Status::ok();
}

auto DBImpl::open_table(TableState &state) -> Status
{
    Page page {state.root_id};
    CDB_TRY(pager->acquire(page));
    const auto checkpoint_lsn = read_checkpoint_lsn(page);
    pager->release(std::move(page));

    state.tree = new Tree {*pager, &state.root_id, m_freelist_head};
    state.checkpoint_lsn = checkpoint_lsn;
    state.is_open = true;
    return Status::ok();
}

auto DBImpl::checkpoint() -> Status
{
    CDB_TRY(m_status);
    if (m_batch_size == 0) {
        return Status::ok();
    }
    Page db_root {LogicalPageId::root()};
    CDB_TRY(pager->acquire(db_root));
    pager->upgrade(db_root);

    auto checkpoint_lsn = wal->current_lsn();

    FileHeader header;
    header.read(db_root.data());
    pager->save_state(header);
    header.freelist_head = m_freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    header.last_table_id = m_last_table_id;
    header.record_count = m_record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());

    auto span = db_root.span(0, FileHeader::kSize);
    header.write(span.data());
    pager->discard(std::move(db_root));

    CDB_TRY(wal->log_commit(root_id, header, nullptr));
    return wal->flush();
    return Status::ok();
}

auto DBImpl::close_table(const LogicalPageId &root_id) -> void
{
    auto *state = m_tables.get(root_id.table_id);
    if (state == nullptr) {
        return;
    }
    auto s = wal->flush();
    if (s.is_ok()) {
        const LogRange log_range {state->checkpoint_lsn, Id::null()};
        s = ensure_consistency({{root_id.table_id, log_range}});
    }

    if (!s.is_ok()) {
        SET_STATUS(s);
    }

    delete state->tree;
    state->tree = nullptr;
    state->is_open = false;
}

auto DBImpl::find_checkpoints(IdMap<LogRange> *ranges) -> Status
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*m_root->tree)};
    cursor->seek_first();

    while (cursor->is_valid()) {
        auto logical_id = LogicalPageId::unknown_page(Id::root());
        CDB_TRY(decode_logical_id(cursor->value(), &logical_id));
        auto itr = ranges->find(logical_id.page_id);
        if (itr != end(*ranges)) {
            return Status::corruption("encountered duplicate root");
        }

        Page page {logical_id};
        CDB_TRY(pager->acquire(page));
        const LogRange range {read_checkpoint_lsn(page), Lsn::null()};
        pager->release(std::move(page));

        ranges->insert(itr, {logical_id.table_id, range});
        cursor->next();
    }
    return Status::ok();
}

static auto apply_undo(Page &page, const ImageDescriptor &image)
{
    const auto data = image.image;
    mem_copy(page.span(0, data.size()), data);
    if (page.size() > data.size()) {
        mem_clear(page.span(data.size(), page.size() - data.size()));
    }
}

static auto apply_redo(Page &page, const DeltaDescriptor &delta)
{
    for (auto [offset, data] : delta.deltas) {
        mem_copy(page.span(offset, data.size()), data);
    }
}

template <class Descriptor, class Callback>
static auto with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
{
    Page page {LogicalPageId {descriptor.table_id, descriptor.page_id}};
    CDB_TRY(pager.acquire(page));

    callback(page);
    pager.release(std::move(page));
    return Status::ok();
}

auto DBImpl::recovery_phase_1(IdMap<LogRange> ranges, Lsn *final_lsn) -> Status
{
    auto &set = wal->m_set;

    if (set.is_empty()) {
        return Status::ok();
    }

    // If no tables are specified in "ranges", run recovery on all open tables.
    if (ranges.empty()) {
        for (const auto &[id, state]: m_tables) {
            if (state.is_open) {
                ranges.emplace(id, LogRange {state.checkpoint_lsn, Lsn::null()});
            }
        }
    }

    // TODO: Need special logic for vacuum. WAL records created during vacuum won't have a valid
    //       table ID field, and we'll need to regenerate the root mappings after applying a
    //       vacuum.
    //       *
    //       Also, if we encounter a vacuum, we would need to reread the whole root tree to
    //       determine the new roots for open tables. To prevent this, we can have an additional
    //       type of WAL record for indicating root moves. Maybe modify the "vacuum start/end" record,
    //       or add a new record type.
//    bool in_vacuum {};

    std::unique_ptr<Reader> file;
    auto segment = set.first();

    const auto translate_status = [&segment, &set](auto s) {
        CDB_EXPECT_FALSE(s.is_ok());
        if (s.is_corruption()) {
            // Allow corruption/incomplete records on the last segment.
            if (segment == set.last()) {
                return Status::ok();
            }
        }
        return s;
    };

    const auto redo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto deltas = std::get<DeltaDescriptor>(decoded);
            auto itr = ranges.find(deltas.table_id);
            if (itr == end(ranges)) {
                // We are not recovering this table right now.
                return Status::ok();
            }
            itr->second.recent_lsn = deltas.lsn;

            // WARNING: Applying these updates can cause the in-memory file header variables to be incorrect. This
            // must be fixed by the caller after this method returns.
            return with_page(*pager, deltas, [this, &deltas](auto &page) {
                if (read_page_lsn(page) < deltas.lsn) {
                    pager->upgrade(page);
                    apply_redo(page, deltas);
                }
            });
        } else if (std::holds_alternative<CommitDescriptor>(decoded)) {
            const auto commit = std::get<CommitDescriptor>(decoded);
            auto itr = ranges.find(commit.table_id);
            if (itr == end(ranges)) {
                // We are not recovering this table right now.
                return Status::ok();
            }
            itr->second.commit_lsn = commit.lsn;
            itr->second.recent_lsn = commit.lsn;

            Page page {LogicalPageId::root()};
            CDB_TRY(pager->acquire(page));
            if (read_page_lsn(page) < commit.lsn) {
                pager->upgrade(page);
                commit.header.write(page.data());
            }
            pager->release(std::move(page));
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted")));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto undo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<ImageDescriptor>(decoded)) {
            const auto image = std::get<ImageDescriptor>(decoded);
            const auto itr = ranges.find(image.table_id);
            if (itr == end(ranges) || image.lsn <= itr->second.commit_lsn) {
                return Status::ok();
            }
            return with_page(*pager, image, [this, &image](auto &page) {
                const auto page_lsn = read_page_lsn(page);
                if (page_lsn.is_null() || page_lsn > image.lsn) {
                    pager->upgrade(page);
                    apply_undo(page, image);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted")));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto roll = [&](const auto &action) {
        CDB_TRY(open_wal_reader(segment, &file));
        WalReader reader {*file, m_reader_tail};

        for (;;) {
            Span buffer {m_reader_data};
            auto s = reader.read(buffer);

            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                return translate_status(s);
            }

            *final_lsn = extract_payload_lsn(buffer);

            s = action(buffer);
            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                return s;
            }
        }
        return Status::ok();
    };

    /* Roll forward, applying missing updates until we reach the end. The final
     * segment may contain a partial/corrupted record.
     */
    for (;; segment = set.id_after(segment)) {
        CDB_TRY(roll(redo));
        if (segment == set.last()) {
            break;
        }
    }

    // Didn't make it to the end of the WAL.
    if (segment != set.last()) {
        return Status::corruption("wal could not be read to the end");
    }

    // Discard entries for tables that ended with checkpoints.
    for (auto itr = begin(ranges); itr != end(ranges); ) {
        const auto [commit_lsn, recent_lsn] = itr->second;
        if (commit_lsn >= recent_lsn) {
            CDB_EXPECT_EQ(commit_lsn, recent_lsn);
            itr = ranges.erase(itr);
        } else {
            ++itr;
        }
    }
    if (ranges.empty()) {
        return Status::ok();
    }

    /* Roll backward, reverting updates until we reach the most-recent commit. We
     * are able to read the log forward, since the full images are disjoint.
     * Again, the last segment we read may contain a partial/corrupted record.
     */
    segment = set.first();
    for (; !segment.is_null(); segment = set.id_after(segment)) {
        CDB_TRY(roll(undo));
    }

    /* Applying WAL records pertaining to a vacuum can cause root page locations to
     * change. We need to update the copies of this variable that we keep in memory for
     * open tables.
     */
    // TODO: We should probably detect root moves while reading the WAL, maybe with a
    //       special WAL record. Then just update the variable right away. Then we don't
    //       need to read the whole root tree again!

    /* "ranges" contains an entry for each table that needed to be rolled back. It is
     * necessary, at this point, to update the checkpoint LSNs on disk for each of
     * these tables. These changes are not recorded in the WAL, but are put to disk
     * immediately (in phase 2). This prevents the obsolete records from being considered
     * again if we were to crash, and lets us clean up the WAL when we're done.
     */
    for (const auto &[table_id, range] : ranges) {
        if (table_id.is_root()) {
            continue;
        }
        auto *state = m_tables.get(table_id);
        CDB_EXPECT_NE(state, nullptr);

        Page table_root {state->root_id};
        CDB_TRY(pager->acquire(table_root));
        pager->upgrade(table_root);
        write_checkpoint_lsn(table_root, range.recent_lsn);
        pager->release(std::move(table_root));
    }
    return Status::ok();
}

auto DBImpl::recovery_phase_2(Lsn recent_lsn) -> Status
{
    auto &set = wal->m_set;
    Page page {LogicalPageId::root()};
    CDB_TRY(pager->acquire(page));

    // Pager needs the updated state to determine the page count.
    FileHeader header;
    header.read(page.data());
    pager->load_state(header);
    pager->release(std::move(page));

    // TODO: This is too expensive for large databases. Look into a WAL index?
    // Make sure we aren't missing any WAL records.
    // for (auto id = Id::root(); id.value <= pager->page_count(); page_id.value++)
    // {
    //    Calico_Try(pager->acquire(Id::root(), page));
    //    const auto lsn = read_page_lsn(page);
    //    pager->release(std::move(page));
    //
    //    if (lsn > *m_commit_lsn) {
    //        return Status::corruption("missing wal updates");
    //    }
    //}

    /* Make sure all changes have made it to disk, then remove WAL segments from
     * the right.
     */
    CDB_TRY(pager->flush());
    for (auto id = set.last(); !id.is_null(); id = set.id_before(id)) {
        CDB_TRY(m_env->remove_file(encode_segment_name(wal->m_prefix, id)));
    }
    set.remove_after(Id::null());

    wal->m_last_lsn = recent_lsn;
    wal->m_flushed_lsn = recent_lsn;
    pager->m_recovery_lsn = recent_lsn;

    // Make sure the file size matches the header page count, which should be
    // correct if we made it this far.
    CDB_TRY(pager->truncate(pager->page_count()));
    return pager->sync();
}

auto DBImpl::open_wal_reader(Id segment, std::unique_ptr<Reader> *out) -> Status
{
    Reader *file;
    const auto name = encode_segment_name(m_wal_prefix, segment);
    CDB_TRY(m_env->new_reader(name, &file));
    out->reset(file);
    return Status::ok();
}

#undef SET_STATUS

} // namespace calicodb

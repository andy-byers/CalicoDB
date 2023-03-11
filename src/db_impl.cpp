
#include "db_impl.h"
#include "calicodb/env.h"
#include "crc.h"
#include "env_posix.h"
#include "logging.h"
#include "wal_reader.h"

namespace calicodb
{

#define SET_STATUS(s)                 \
    do {                              \
        if (m_state.status.is_ok()) { \
            m_state.status = s;       \
        }                             \
    } while (0)

static auto get_table_id(const Table *table) -> Id
{
    return reinterpret_cast<const TableImpl *>(table)->id();
}

TableImpl::TableImpl(const TableOptions &options, std::string name, Id table_id)
    : m_options {options},
      m_name {std::move(name)},
      m_id {table_id}
{
}

TableSet::~TableSet()
{
    for (const auto *state : m_tables) {
        if (state != nullptr) {
            delete state->tree;
            delete state;
        }
    }
}

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
    if (table_id.as_index() >= m_tables.size()) {
        return nullptr;
    }
    return m_tables[table_id.as_index()];
}

auto TableSet::get(Id table_id) -> TableState *
{
    if (table_id.as_index() >= m_tables.size()) {
        return nullptr;
    }
    return m_tables[table_id.as_index()];
}

auto TableSet::add(const LogicalPageId &root_id) -> void
{
    const auto index = root_id.table_id.as_index();
    while (index >= m_tables.size()) {
        m_tables.emplace_back(nullptr);
    }
    if (m_tables[index] == nullptr) {
        m_tables[index] = new TableState;
        m_tables[index]->root_id = root_id;
    }
}

auto TableSet::erase(Id table_id) -> void
{
    const auto index = table_id.as_index();
    if (m_tables[index] != nullptr) {
        delete m_tables[index]->tree;
        delete m_tables[index];
    }
    m_tables[index] = nullptr;
}

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

    m_commit_lsn = state.commit_lsn;
    m_state.record_count = state.record_count;
    m_freelist_head = state.freelist_head;

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
            &m_commit_lsn,
            &m_state.status,
            &m_is_running,
            sanitized.cache_size / page_size,
            page_size,
        },
        &pager));

    if (!db_exists) {
        m_info_log->logv("setting up a new database");

        // Create the root tree.
        Id root_id;
        CDB_TRY(Tree::create(*pager, Id::root(), m_freelist_head, &root_id));
        CDB_EXPECT_TRUE(root_id.is_root());
    }
    pager->load_state(state);

    // Create the root and default table handles.
    CDB_TRY(create_table({}, kRootTableName, &m_root));
    CDB_TRY(create_table({}, kDefaultTableName, &m_default));

    auto *cursor = new_cursor(m_root);
    cursor->seek_first();
    while (cursor->is_valid()) {
        LogicalPageId root_id;
        CDB_TRY(decode_logical_id(cursor->value(), &root_id));
        m_tables.add(root_id);
        cursor->next();
    }
    delete cursor;

    if (db_exists) {
        m_info_log->logv("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CDB_TRY(ensure_consistency());
    } else {
        // Write the initial file header.
        Page db_root;
        CDB_TRY(pager->acquire(Id::root(), &db_root));
        pager->upgrade(db_root);
        state.page_count = pager->page_count();
        state.header_crc = crc32c::Mask(state.compute_crc());
        state.write(db_root.span(0, FileHeader::kSize).data());
        pager->release(std::move(db_root));
        CDB_TRY(pager->flush());
    }
    CDB_TRY(wal->start_writing());

    m_info_log->logv("pager recovery lsn is %llu", pager->recovery_lsn().value);
    m_info_log->logv("wal flushed lsn is %llu", wal->flushed_lsn().value);

    CDB_TRY(m_state.status);
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
      m_owns_env {options.env == nullptr},
      m_owns_info_log {options.info_log == nullptr}
{
}

DBImpl::~DBImpl()
{
    if (m_is_running && m_state.status.is_ok()) {
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

    delete m_default;
    delete m_root;
    delete pager;
    delete wal;
}

auto DBImpl::record_count() const -> std::size_t
{
    return m_state.record_count;
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
    Reader *reader {};
    auto s = env->new_reader(path, &reader);

    if (s.is_ok()) {
        char read_buffer[FileHeader::kSize];
        auto read_size = sizeof(read_buffer);
        s = reader->read(read_buffer, &read_size, 0);
        if (s.is_ok() && read_size != sizeof(read_buffer)) {
            s = Status::invalid_argument(path + " is too small to be a calicodb database");
        }
        if (s.is_ok()) {
            FileHeader header;
            header.read(read_buffer);
            if (header.magic_code != FileHeader::kMagicCode) {
                s = Status::invalid_argument(path + " is not a calicodb database");
            }
        }
    }

    if (s.is_ok()) {
        s = env->remove_file(path);

        std::vector<std::string> children;
        auto t = env->get_children(dir, &children);
        if (t.is_ok()) {
            for (const auto &name : children) {
                const auto sibling_filename = join_paths(dir, name);
                const auto possible_id = decode_segment_name(wal_prefix, sibling_filename);
                if (!possible_id.is_null()) {
                    auto u = env->remove_file(sibling_filename);
                    if (t.is_ok()) {
                        t = u;
                    }
                }
            }
        }
        if (s.is_ok()) {
            s = t;
        }
    }

    delete reader;
    if (owns_env) {
        delete env;
    }
    return s;
}

auto DBImpl::status() const -> Status
{
    return m_state.status;
}

auto DBImpl::get_property(const Slice &name, std::string *out) const -> bool
{
    if (name.starts_with("calicodb.")) {
        const auto prop = name.range(std::strlen("calicodb."));

        if (prop == "tables") {
            // TODO: This should provide information about open tables, or maybe all tables.
            out->append("<NOT IMPLEMENTED>");
            return true;

        } else if (prop == "stats") {
            // TODO: This should provide information about how much data was written to/read from different files,
            //       number of cache hits and misses, and maybe other things.
            out->append("<NOT IMPLEMENTED>");
            return true;
        }
    }
    return false;
}

auto DBImpl::list_tables(std::vector<std::string> *out) const -> Status
{
    CDB_TRY(m_state.status);
    out->clear();

    auto *cursor = new_cursor(m_root);
    cursor->seek_first();
    while (cursor->is_valid()) {
        out->emplace_back(cursor->key().to_string());
        cursor->next();
    }
    auto s = cursor->status();
    delete cursor;

    return s.is_not_found() ? Status::ok() : s;
}


auto DBImpl::new_cursor(const Table *table) const -> Cursor *
{
    CDB_EXPECT_NE(table, nullptr);
    const auto *state = m_tables.get(get_table_id(table));
    auto *cursor = CursorInternal::make_cursor(*state->tree);
    if (!m_state.status.is_ok()) {
        CursorInternal::invalidate(*cursor, m_state.status);
    }
    return cursor;
}

auto DBImpl::get(const Table *table, const Slice &key, std::string *value) const -> Status
{
    CDB_EXPECT_NE(table, nullptr);
    CDB_TRY(m_state.status);
    const auto *state = m_tables.get(get_table_id(table));
    return state->tree->get(key, value);
}

auto DBImpl::put(Table *table, const Slice &key, const Slice &value) -> Status
{
    CDB_TRY(m_state.status);

    CDB_EXPECT_NE(table, nullptr);
    auto *state = m_tables.get(get_table_id(table));

    if (!state->write) {
        return Status::logic_error("table is not writable");
    }
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }

    bool record_exists;
    if (auto s = state->tree->put(key, value, &record_exists); !s.is_ok()) {
        SET_STATUS(s);
        return s;
    }
    m_state.record_count += !record_exists;
    m_state.bytes_written += key.size() + value.size();
    ++m_state.batch_size;
    return Status::ok();
}

auto DBImpl::erase(Table *table, const Slice &key) -> Status
{
    CDB_TRY(m_state.status);

    CDB_EXPECT_NE(table, nullptr);
    auto *state = m_tables.get(get_table_id(table));

    if (!state->write) {
        return Status::logic_error("table is not writable");
    }

    auto s = state->tree->erase(key);
    if (s.is_ok()) {
        ++m_state.batch_size;
        --m_state.record_count;
    } else if (!s.is_not_found()) {
        SET_STATUS(s);
    }
    return s;
}

auto DBImpl::vacuum() -> Status
{
    CDB_TRY(m_state.status);
    if (auto s = do_vacuum(); !s.is_ok()) {
        SET_STATUS(s);
    }
    return m_state.status;
}

auto DBImpl::do_vacuum() -> Status
{
    Id target {pager->page_count()};
    if (target.is_root()) {
        return Status::ok();
    }
    auto *state = m_tables.get(Id::root());
    auto *tree = state->tree;

    const auto original = target;
    for (;; target.value--) {
        bool vacuumed;
        CDB_TRY(tree->vacuum_one(target, m_tables, &vacuumed));
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

auto DBImpl::ensure_consistency() -> Status
{
    m_is_running = false;
    CDB_TRY(recovery_phase_1());
    CDB_TRY(recovery_phase_2());
    m_is_running = true;
    return load_file_header();
}

auto DBImpl::load_file_header() -> Status
{
    Page root;
    CDB_TRY(pager->acquire(Id::root(), &root));

    FileHeader header;
    header.read(root.data());
    const auto expected_crc = crc32c::Unmask(header.header_crc);
    const auto computed_crc = header.compute_crc();
    if (expected_crc != computed_crc) {
        m_info_log->logv("file header crc mismatch (expected %u but computed %u)", expected_crc, computed_crc);
        return Status::corruption("crc mismatch");
    }

    m_state.record_count = header.record_count;
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
    for (const auto &state : m_tables) {
        if (state && state->open) {
            state->tree->TEST_validate();
        }
    }
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
        header->page_size = static_cast<std::uint16_t>(options.page_size);
        header->header_crc = crc32c::Mask(header->compute_crc());
        header->page_count = 1;

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

auto DBImpl::checkpoint() -> Status
{
    if (m_state.batch_size != 0) {
        if (auto s = save_file_header(); !s.is_ok()) {
            SET_STATUS(s);
            return s;
        }
    }
    return Status::ok();
}

auto DBImpl::save_file_header() -> Status
{
    CDB_TRY(m_state.status);
    Page db_root;
    CDB_TRY(pager->acquire(Id::root(), &db_root));
    pager->upgrade(db_root);

    FileHeader header;
    header.read(db_root.data());
    pager->save_state(header);
    header.freelist_head = m_freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    header.commit_lsn = wal->current_lsn();
    m_commit_lsn = wal->current_lsn();
    header.record_count = m_state.record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());
    header.write(db_root.span(0, FileHeader::kSize).data());
    pager->release(std::move(db_root));

    return wal->flush();
}

auto DBImpl::default_table() const -> Table *
{
    return m_default;
}

auto DBImpl::create_table(const TableOptions &options, const std::string &name, Table **out) -> Status
{
    LogicalPageId root_id;
    std::string value;
    Status s;

    if (name == kRootTableName) {
        root_id = LogicalPageId::root();
    } else {
        auto *state = m_tables.get(Id::root());
        s = state->tree->get(name, &value);
        if (s.is_ok()) {
            CDB_TRY(decode_logical_id(value, &root_id));
        } else if (s.is_not_found()) {
            s = construct_new_table(name, &root_id);
        }
    }

    if (!s.is_ok()) {
        SET_STATUS(s);
        return s;
    }

    m_tables.add(root_id);
    auto *state = m_tables.get(root_id.table_id);
    CDB_EXPECT_NE(state, nullptr);

    if (state->open) {
        return Status::invalid_argument("table is already open");
    }
    state->tree = new Tree {*pager, root_id.page_id, m_freelist_head};
    state->write = options.mode == AccessMode::kReadWrite;
    state->open = true;
    *out = new TableImpl {options, name, root_id.table_id};

    return s;
}

auto DBImpl::close_table(Table *table) -> void
{
    if (table == nullptr || table == default_table()) {
        return;
    }
    auto *state = m_tables.get(get_table_id(table));
    CDB_EXPECT_NE(state, nullptr);

    delete state->tree;
    state->tree = nullptr;
    state->write = false;
    state->open = false;
    delete table;
}

auto DBImpl::drop_table(Table *table) -> Status
{
    if (table == nullptr) {
        return Status::ok();
    } else if (table == default_table()) {
        return Status::invalid_argument("cannot drop default table");
    }
    const auto table_id = get_table_id(table);
    auto *cursor = new_cursor(table);
    Status s;

    while (s.is_ok()) {
        cursor->seek_first();
        if (!cursor->is_valid()) {
            break;
        }
        s = erase(table, cursor->key());
    }
    delete cursor;

    auto *state = m_tables.get(table_id);
    s = remove_empty_table(table->name(), *state);
    if (!s.is_ok()) {
        SET_STATUS(s);
    }
    delete table;
    m_tables.erase(table_id);
    return s;
}

auto DBImpl::construct_new_table(const Slice &name, LogicalPageId *root_id) -> Status
{
    // Find the first available table ID.
    auto table_id = Id::root();
    for (const auto &itr : m_tables) {
        if (itr == nullptr) {
            break;
        }
        ++table_id.value;
    }

    // Set the table ID manually, let the tree fill in the root page ID.
    root_id->table_id = table_id;
    CDB_TRY(Tree::create(*pager, table_id, m_freelist_head, &root_id->page_id));

    char payload[LogicalPageId::kSize];
    encode_logical_id(*root_id, payload);

    // Write an entry for the new table in the root table.
    auto *db_root = m_tables.get(Id::root());
    CDB_TRY(db_root->tree->put(name, Slice {payload, LogicalPageId::kSize}));
    ++m_state.batch_size;
    return Status::ok();
}

auto DBImpl::remove_empty_table(const std::string &name, TableState &state) -> Status
{
    auto &[root_id, tree, write_flag, open_flag] = state;
    if (root_id.table_id.is_root()) {
        return Status::ok();
    }

    Node root;
    CDB_TRY(tree->acquire(&root, root_id.page_id, false));
    if (root.header.cell_count != 0) {
        return Status::logic_error("table is not empty");
    }
    auto *root_state = m_tables.get(Id::root());
    CDB_TRY(root_state->tree->erase(name));
    tree->upgrade(root);
    CDB_TRY(tree->destroy(std::move(root)));
    return Status::ok();
}

static auto apply_undo(Page &page, const ImageDescriptor &image)
{
    const auto data = image.image;
    std::memcpy(page.data(), data.data(), data.size());
    if (page.size() > data.size()) {
        std::memset(page.data() + data.size(), 0, page.size() - data.size());
    }
}

static auto apply_redo(Page &page, const DeltaDescriptor &delta)
{
    for (auto [offset, data] : delta.deltas) {
        std::memcpy(page.data() + offset, data.data(), data.size());
    }
}

template <class Descriptor, class Callback>
static auto with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
{
    Page page;
    CDB_TRY(pager.acquire(descriptor.page_id, &page));

    callback(page);
    pager.release(std::move(page));
    return Status::ok();
}

static auto is_commit(const DeltaDescriptor &deltas)
{
    return deltas.page_id.is_root() && deltas.deltas.size() == 1 && deltas.deltas.front().offset == 0 &&
           deltas.deltas.front().data.size() == FileHeader::kSize + sizeof(Lsn);
}

auto DBImpl::recovery_phase_1() -> Status
{
    const auto &set = wal->m_set;

    if (set.is_empty()) {
        return Status::ok();
    }

    std::unique_ptr<Reader> file;
    auto segment = set.first();
    auto commit_lsn = m_commit_lsn;
    auto commit_segment = segment;
    Lsn last_lsn;

    const auto translate_status = [&segment, &set, this](auto s, Lsn lsn) {
        CDB_EXPECT_FALSE(s.is_ok());
        if (s.is_corruption()) {
            // Allow corruption/incomplete records on the last segment, past the
            // most-recent successful commit.
            if (segment == set.last() && lsn >= m_commit_lsn) {
                return Status::ok();
            }
        }
        return s;
    };

    const auto redo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
            const auto deltas = std::get<DeltaDescriptor>(decoded);
            if (is_commit(deltas)) {
                commit_lsn = deltas.lsn;
                commit_segment = segment;
            }
            // WARNING: Applying these updates can cause the in-memory file header variables to be incorrect. This
            // must be fixed by the caller after this method returns.
            return with_page(*pager, deltas, [this, &deltas](auto &page) {
                if (read_page_lsn(page) < deltas.lsn) {
                    pager->upgrade(page);
                    apply_redo(page, deltas);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto undo = [&](const auto &payload) {
        const auto decoded = decode_payload(payload);
        if (std::holds_alternative<ImageDescriptor>(decoded)) {
            const auto image = std::get<ImageDescriptor>(decoded);
            return with_page(*pager, image, [this, &image](auto &page) {
                if (image.lsn < m_commit_lsn) {
                    return;
                }
                const auto page_lsn = read_page_lsn(page);
                if (page_lsn.is_null() || page_lsn > image.lsn) {
                    pager->upgrade(page);
                    apply_undo(page, image);
                }
            });
        } else if (std::holds_alternative<std::monostate>(decoded)) {
            CDB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
            return Status::not_found("finished");
        }
        return Status::ok();
    };

    const auto roll = [&](const auto &action) {
        CDB_TRY(open_wal_reader(segment, &file));
        WalReader reader {*file, m_reader_tail};

        for (;;) {
            Span payload {m_reader_data};
            auto s = reader.read(payload);

            if (s.is_not_found()) {
                break;
            } else if (!s.is_ok()) {
                CDB_TRY(translate_status(s, last_lsn));
                return Status::ok();
            }

            last_lsn = extract_payload_lsn(payload);

            s = action(payload);
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

    if (last_lsn == commit_lsn) {
        if (m_commit_lsn <= commit_lsn) {
            m_commit_lsn = commit_lsn;
            return Status::ok();
        } else {
            return Status::corruption("missing commit record");
        }
    }
    m_commit_lsn = commit_lsn;

    /* Roll backward, reverting updates until we reach the most-recent commit. We
     * are able to read the log forward, since the full images are disjoint.
     * Again, the last segment we read may contain a partial/corrupted record.
     */
    segment = commit_segment;
    for (; !segment.is_null(); segment = set.id_after(segment)) {
        CDB_TRY(roll(undo));
    }
    return Status::ok();
}

auto DBImpl::recovery_phase_2() -> Status
{
    auto &set = wal->m_set;
    // Pager needs the updated state to determine the page count.
    Page page;
    CDB_TRY(pager->acquire(Id::root(), &page));
    FileHeader header;
    header.read(page.data());
    pager->load_state(header);
    pager->release(std::move(page));

    // TODO: This is too expensive for large databases. Look into a WAL index?
    // Make sure we aren't missing any WAL records.
    // for (auto id = Id::root(); id.value <= pager->page_count(); id.value++)
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

    wal->m_last_lsn = m_commit_lsn;
    wal->m_flushed_lsn = m_commit_lsn;
    pager->m_recovery_lsn = m_commit_lsn;

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

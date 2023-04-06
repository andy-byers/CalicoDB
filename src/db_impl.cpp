// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "calicodb/env.h"
#include "env_posix.h"
#include "logging.h"

namespace calicodb
{

#define SET_STATUS(s)                 \
    do {                              \
        if (m_state.status.is_ok()) { \
            m_state.status = s;       \
        }                             \
    } while (0)

static auto get_table_id(const Table &table) -> Id
{
    return reinterpret_cast<const TableImpl &>(table).id();
}

static auto encode_page_size(std::size_t page_size) -> U16
{
    return page_size < kMaxPageSize ? static_cast<U16>(page_size) : 0;
}

static auto decode_page_size(unsigned header_page_size) -> U32
{
    return header_page_size > 0 ? header_page_size : kMaxPageSize;
}

Table::~Table() = default;

TableImpl::TableImpl(std::string name, Id table_id)
    : m_name(std::move(name)),
      m_id(table_id)
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
    if (m_tables.size() <= index) {
        m_tables.resize(index + 1);
    }
    // Table slot must not be occupied.
    CALICODB_EXPECT_EQ(m_tables[index], nullptr);
    m_tables[index] = new TableState;
    m_tables[index]->root_id = root_id;
}

auto TableSet::erase(Id table_id) -> void
{
    const auto index = table_id.as_index();
    if (m_tables[index] != nullptr) {
        delete m_tables[index]->tree;
        delete m_tables[index];
        m_tables[index] = nullptr;
    }
}

static auto encode_logical_id(LogicalPageId id, char *out) -> void
{
    put_u32(out, id.table_id.value);
    put_u32(out + Id::kSize, id.page_id.value);
}

[[nodiscard]] static auto decode_logical_id(const Slice &in, LogicalPageId *out) -> Status
{
    if (in.size() != LogicalPageId::kSize) {
        return Status::corruption("logical id is corrupted");
    }
    out->table_id.value = get_u32(in.data());
    out->page_id.value = get_u32(in.data() + Id::kSize);
    return Status::ok();
}

auto DBImpl::open(Options sanitized) -> Status
{
    CALICODB_EXPECT_GE(sanitized.page_size, kMinPageSize);
    CALICODB_EXPECT_LE(sanitized.page_size, kMaxPageSize);
    CALICODB_EXPECT_TRUE(is_power_of_two(sanitized.page_size));
    FileHeader header;

    const auto db_exists = m_env->file_exists(m_db_filename);
    if (db_exists) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument("database already exists");
        }
        File *reader;
        char buffer[FileHeader::kSize];
        CALICODB_TRY(m_env->new_file(m_db_filename, reader));
        CALICODB_TRY(reader->read_exact(0, sizeof(buffer), buffer));
        delete reader;
        if (!header.read(buffer)) {
            return Status::invalid_argument("file is not a CalicoDB database");
        }
    } else if (!sanitized.create_if_missing) {
        return Status::invalid_argument("database does not exist");
    } else {
        header.page_size = encode_page_size(sanitized.page_size);
    }
    const auto page_size = decode_page_size(header.page_size);
    const auto cache_size = std::max(sanitized.cache_size, kMinFrameCount * page_size);
    m_state.freelist_head.value = header.freelist_head;

    const Wal::Parameters wal_param = {
        m_wal_filename,
        page_size,
        m_env,
    };
    CALICODB_TRY(Wal::open(wal_param, m_wal));

    const Pager::Parameters pager_param = {
        m_db_filename,
        m_env,
        m_wal,
        m_log,
        &m_state,
        cache_size / page_size,
        page_size,
    };
    CALICODB_TRY(Pager::open(pager_param, &m_pager));
    m_pager->load_state(header);

    if (!db_exists) {
        m_log->logv("setting up a new database");

        // Create the root tree.
        Id root_id;
        CALICODB_TRY(Tree::create(*m_pager, Id::root(), m_state.freelist_head, &root_id));
        CALICODB_EXPECT_TRUE(root_id.is_root());
    }

    // Create the root and default table handles.
    CALICODB_TRY(create_table(TableOptions(), kRootTableName, m_root));
    CALICODB_TRY(create_table(TableOptions(), kDefaultTableName, m_default));

    auto *cursor = new_cursor(*m_root);
    cursor->seek_first();
    while (cursor->is_valid()) {
        LogicalPageId root_id;
        CALICODB_TRY(decode_logical_id(cursor->value(), &root_id));
        if (m_tables.get(root_id.table_id) == nullptr) {
            m_tables.add(root_id);
        }
        cursor->next();
    }
    delete cursor;

    if (db_exists) {
        m_log->logv("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CALICODB_TRY(checkpoint_if_needed(true));
        CALICODB_TRY(load_file_header());
    } else {
        // Write the initial file header.
        Page db_root;
        CALICODB_TRY(m_pager->acquire(Id::root(), db_root));
        m_pager->upgrade(db_root);
        header.page_count = static_cast<U32>(m_pager->page_count());
        header.write(db_root.data());
        m_pager->release(std::move(db_root));
        CALICODB_TRY(m_pager->flush_to_disk());
    }
    CALICODB_TRY(m_state.status);
    m_state.use_wal = true;
    return Status::ok();
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_env(sanitized.env),
      m_log(sanitized.info_log),
      m_db_filename(std::move(filename)),
      m_wal_filename(sanitized.wal_filename),
      m_log_filename(sanitized.info_log == nullptr ? m_db_filename + kDefaultLogSuffix : ""),
      m_owns_env(options.env == nullptr),
      m_owns_log(options.info_log == nullptr),
      m_sync(options.sync)
{
}

DBImpl::~DBImpl()
{
    if (m_state.use_wal && m_state.status.is_ok()) {
        if (const auto s = m_pager->abort(); !s.is_ok()) {
            m_log->logv("failed to revert uncommitted transaction: %s", s.to_string().c_str());
        }
        if (const auto s = checkpoint_if_needed(true); !s.is_ok()) {
            m_log->logv("failed to checkpoint database: %s", s.to_string().c_str());
        }
        if (const auto s = Wal::close(m_wal); !s.is_ok()) {
            m_log->logv("failed to close WAL: %s", s.to_string().c_str());
        }
    }

    if (m_owns_log) {
        delete m_log;
    }
    if (m_owns_env) {
        delete m_env;
    }

    delete m_default;
    delete m_root;
    delete m_pager;
    delete m_wal;
}

auto DBImpl::repair(const Options &options, const std::string &filename) -> Status
{
    (void)filename;
    (void)options;
    return Status::not_supported("<NOT IMPLEMENTED>"); // TODO: repair() operation attempts to fix a
                                                       // database that could not be opened due to
                                                       // corruption that couldn't/shouldn't be rolled
                                                       // back.
}

auto DBImpl::destroy(const Options &options, const std::string &filename) -> Status
{
    auto copy = options;
    copy.error_if_exists = false;
    copy.create_if_missing = false;

    DB *db;
    auto s = DB::open(copy, filename, db);
    if (!s.is_ok()) {
        return Status::invalid_argument(filename + " is not a CalicoDB database");
    }

    const auto *impl = reinterpret_cast<const DBImpl *>(db);
    const auto db_name = impl->m_db_filename;
    const auto log_name = impl->m_log_filename;
    const auto wal_name = impl->m_wal_filename;
    delete db;

    auto *env = options.env;
    if (env == nullptr) {
        env = Env::default_env();
    }

    if (!log_name.empty()) {
        (void)env->remove_file(log_name);
    }
    (void)env->remove_file(db_name);
    (void)env->remove_file(wal_name);

    if (env != options.env) {
        delete env;
    }

    return Status::ok();
}

auto DBImpl::status() const -> Status
{
    return m_state.status;
}

auto DBImpl::get_property(const Slice &name, std::string *out) const -> bool
{
    if (name.starts_with("calicodb.")) {
        const auto prop = name.range(std::strlen("calicodb."));
        std::string buffer;

        if (prop == "stats") {
            if (out != nullptr) {
                write_to_string(
                    buffer,
                    "Name          Value\n"
                    "-------------------\n"
                    "Pager I/O(MB) %8.4f/%8.4f\n"
                    "WAL I/O(MB)   %8.4f/%8.4f\n"
                    "Cache hits    %ld\n"
                    "Cache misses  %ld\n",
                    static_cast<double>(m_pager->bytes_read()) / 1048576.0,
                    static_cast<double>(m_pager->bytes_written()) / 1048576.0,
                    static_cast<double>(m_wal->statistics().bytes_read) / 1048576.0,
                    static_cast<double>(m_wal->statistics().bytes_written) / 1048576.0,
                    m_pager->hits(),
                    m_pager->misses());
                out->append(buffer);
            }
            return true;
        } else if (prop == "tables") {
            if (out != nullptr) {
                out->append(
                    "Name             SMOCount Read(MB) Write(MB)\n"
                    "--------------------------------------------\n");
                std::vector<std::string> table_names;
                std::vector<LogicalPageId> table_roots;
                (void)get_table_info(table_names, &table_roots);
                table_names.emplace_back(m_default->name());
                table_roots.emplace_back(LogicalPageId::with_table(Id(2)));
                for (std::size_t i = 0; i < table_names.size(); ++i) {
                    const auto *state = m_tables.get(table_roots[i].table_id);
                    if (table_names[i].size() > 16) {
                        table_names[i].resize(13);
                        table_names[i].append("...");
                    }
                    if (state != nullptr && state->open) {
                        const auto n = write_to_string(
                            buffer,
                            "%-16s %8u %8.4f %9.4lf\n",
                            table_names[i].c_str(),
                            state->stats.smo_count,
                            static_cast<double>(state->stats.bytes_read) / 1048576.0,
                            static_cast<double>(state->stats.bytes_written) / 1048576.0);
                        buffer.resize(n);
                        out->append(buffer);
                    }
                }
            }
            return true;
        }
    }
    return false;
}

auto DBImpl::new_cursor(const Table &table) const -> Cursor *
{
    const auto *state = m_tables.get(get_table_id(table));
    CALICODB_EXPECT_NE(state, nullptr);
    auto *cursor = CursorInternal::make_cursor(*state->tree);
    if (!m_state.status.is_ok()) {
        CursorInternal::invalidate(*cursor, m_state.status);
    }
    return cursor;
}

auto DBImpl::get(const Table &table, const Slice &key, std::string *value) const -> Status
{
    CALICODB_TRY(m_state.status);
    const auto *state = m_tables.get(get_table_id(table));
    CALICODB_EXPECT_NE(state, nullptr);
    return state->tree->get(key, value);
}

auto DBImpl::put(Table &table, const Slice &key, const Slice &value) -> Status
{
    CALICODB_TRY(m_state.status);
    auto *state = m_tables.get(get_table_id(table));
    CALICODB_EXPECT_NE(state, nullptr);

    if (!state->write) {
        return Status::invalid_argument("table is not writable");
    }
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }

    bool record_exists;
    auto s = state->tree->put(key, value, &record_exists);
    if (s.is_ok()) {
        ++m_state.batch_size;
    } else {
        SET_STATUS(s);
    }
    return s;
}

auto DBImpl::erase(Table &table, const Slice &key) -> Status
{
    CALICODB_TRY(m_state.status);
    auto *state = m_tables.get(get_table_id(table));
    CALICODB_EXPECT_NE(state, nullptr);

    if (!state->write) {
        return Status::invalid_argument("table is not writable");
    }

    auto s = state->tree->erase(key);
    if (s.is_ok()) {
        ++m_state.batch_size;
    } else if (!s.is_not_found()) {
        SET_STATUS(s);
    }
    return s;
}

auto DBImpl::vacuum() -> Status
{
    CALICODB_TRY(m_state.status);
    if (auto s = do_vacuum(); !s.is_ok()) {
        SET_STATUS(s);
    }
    return m_state.status;
}

auto DBImpl::do_vacuum() -> Status
{
    std::vector<std::string> table_names;
    std::vector<LogicalPageId> table_roots;
    CALICODB_TRY(get_table_info(table_names, &table_roots));

    Id target(m_pager->page_count());
    auto *state = m_tables.get(Id::root());
    auto *tree = state->tree;

    const auto original = target;
    for (;; --target.value) {
        bool vacuumed;
        CALICODB_TRY(tree->vacuum_one(target, m_tables, &vacuumed));
        if (!vacuumed) {
            break;
        }
    }
    if (target.value == m_pager->page_count()) {
        // No pages available to vacuum: database is minimally sized.
        return Status::ok();
    }

    // Update root locations in the name-to-root mapping.
    char logical_id[LogicalPageId::kSize];
    for (std::size_t i = 0; i < table_names.size(); ++i) {
        const auto *root = m_tables.get(table_roots[i].table_id);
        CALICODB_EXPECT_NE(root, nullptr);
        encode_logical_id(root->root_id, logical_id);
        CALICODB_TRY(put(*m_root, table_names[i], logical_id));
    }
    CALICODB_TRY(m_pager->resize(target.value));

    m_log->logv("vacuumed %llu pages", original.value - target.value);
    return Status::ok();
}

auto DBImpl::TEST_wal() const -> const Wal &
{
    return *m_wal;
}

auto DBImpl::TEST_pager() const -> const Pager &
{
    return *m_pager;
}

auto DBImpl::TEST_tables() const -> const TableSet &
{
    return m_tables;
}

auto DBImpl::TEST_state() const -> const DBState &
{
    return m_state;
}

auto DBImpl::TEST_validate() const -> void
{
    for (const auto *state : m_tables) {
        if (state != nullptr && state->open) {
            state->tree->TEST_validate();
        }
    }
}

auto DBImpl::commit() -> Status
{
    CALICODB_TRY(m_state.status);
    if (auto s = do_commit(); !s.is_ok()) {
        SET_STATUS(s);
        return s;
    }
    return Status::ok();
}

auto DBImpl::do_commit() -> Status
{
    Page db_root;
    CALICODB_TRY(m_pager->acquire(Id::root(), db_root));

    FileHeader header;
    header.read(db_root.data());

    const auto needs_new_header =
        header.page_count != m_pager->page_count() ||
        header.freelist_head != m_state.freelist_head.value;
    if (needs_new_header) {
        m_pager->upgrade(db_root);
        header.page_count = static_cast<U32>(m_pager->page_count());
        header.freelist_head = m_state.freelist_head.value;
        header.write(db_root.data());
    }
    m_pager->release(std::move(db_root));

    // Write all dirty pages to the WAL.
    CALICODB_TRY(m_pager->commit());

    if (m_sync) {
        CALICODB_TRY(m_wal->sync());
    }
    return checkpoint_if_needed();
}

auto DBImpl::checkpoint_if_needed(bool is_recovery) -> Status
{
    if (is_recovery || m_wal->needs_checkpoint()) {
        CALICODB_TRY(m_pager->checkpoint_phase_1(is_recovery));
        CALICODB_TRY(load_file_header());
        CALICODB_TRY(m_pager->checkpoint_phase_2());
    }
    return Status::ok();
}

auto DBImpl::load_file_header() -> Status
{
    Page root;
    CALICODB_TRY(m_pager->acquire(Id::root(), root));

    FileHeader header;
    if (!header.read(root.data())) {
        return Status::corruption("header identifier mismatch");
    }
    m_state.freelist_head.value = header.freelist_head;
    m_pager->load_state(header);

    m_pager->release(std::move(root));
    return Status::ok();
}

auto DBImpl::default_table() const -> Table *
{
    return m_default;
}

auto DBImpl::get_table_info(std::vector<std::string> &names, std::vector<LogicalPageId> *roots) const -> Status
{
    names.clear();
    if (roots != nullptr) {
        roots->clear();
    }

    auto *cursor = new_cursor(*m_root);
    cursor->seek_first();
    while (cursor->is_valid()) {
        if (cursor->key() != kDefaultTableName) {
            names.emplace_back(cursor->key().to_string());
            if (roots != nullptr) {
                LogicalPageId root;
                CALICODB_TRY(decode_logical_id(cursor->value(), &root));
                roots->emplace_back(root);
            }
        }
        cursor->next();
    }
    auto s = cursor->status();
    delete cursor;

    return s.is_not_found() ? Status::ok() : s;
}

auto DBImpl::list_tables(std::vector<std::string> &out) const -> Status
{
    CALICODB_TRY(m_state.status);
    return get_table_info(out, nullptr);
}

auto DBImpl::create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    LogicalPageId root_id;
    std::string value;
    Status s;

    if (name == kRootTableName) {
        // Root table should be closed, i.e. we should be in open(). Attempting to open the
        // root table again will result in undefined behavior.
        CALICODB_EXPECT_EQ(m_tables.get(Id::root()), nullptr);
        root_id = LogicalPageId::root();
    } else {
        const auto *state = m_tables.get(Id::root());
        s = state->tree->get(name, &value);
        if (s.is_ok()) {
            CALICODB_TRY(decode_logical_id(value, &root_id));
        } else if (s.is_not_found()) {
            s = construct_new_table(name, root_id);
        }
    }

    if (!s.is_ok()) {
        SET_STATUS(s);
        return s;
    }

    auto *state = m_tables.get(root_id.table_id);
    if (state == nullptr) {
        m_tables.add(root_id);
        state = m_tables.get(root_id.table_id);
    }
    CALICODB_EXPECT_NE(state, nullptr);

    if (state->open) {
        return Status::invalid_argument("table is already open");
    }
    state->tree = new Tree(*m_pager, root_id.page_id, m_state.freelist_head, &state->stats);
    state->write = options.mode == AccessMode::kReadWrite;
    state->open = true;
    out = new TableImpl(name, root_id.table_id);

    return s;
}

auto DBImpl::close_table(Table *&table) -> void
{
    if (table == nullptr || table == default_table()) {
        return;
    }
    auto *state = m_tables.get(get_table_id(*table));
    CALICODB_EXPECT_NE(state, nullptr);

    delete state->tree;
    state->tree = nullptr;
    state->write = false;
    state->open = false;
    delete table;
}

auto DBImpl::drop_table(Table *&table) -> Status
{
    if (table == nullptr) {
        return Status::ok();
    } else if (table == default_table()) {
        return Status::invalid_argument("cannot drop default table");
    }
    const auto table_id = get_table_id(*table);
    auto *cursor = new_cursor(*table);
    Status s;

    while (s.is_ok()) {
        cursor->seek_first();
        if (!cursor->is_valid()) {
            break;
        }
        s = erase(*table, cursor->key());
    }
    delete cursor;

    auto *state = m_tables.get(table_id);
    s = remove_empty_table(table->name(), *state);
    if (!s.is_ok()) {
        SET_STATUS(s);
    }
    delete table;
    m_tables.erase(table_id);
    ++m_state.batch_size;
    return s;
}

auto DBImpl::construct_new_table(const Slice &name, LogicalPageId &root_id) -> Status
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
    root_id.table_id = table_id;
    CALICODB_TRY(Tree::create(*m_pager, table_id, m_state.freelist_head, &root_id.page_id));

    char payload[LogicalPageId::kSize];
    encode_logical_id(root_id, payload);

    // Write an entry for the new table in the root table. This will not increase the
    // record count for the database.
    auto *db_root = m_tables.get(Id::root());
    CALICODB_TRY(db_root->tree->put(name, Slice(payload, LogicalPageId::kSize)));
    ++m_state.batch_size;
    return Status::ok();
}

auto DBImpl::remove_empty_table(const std::string &name, TableState &state) -> Status
{
    auto &[root_id, stats, tree, write_flag, open_flag] = state;
    if (root_id.table_id.is_root()) {
        return Status::ok();
    }

    Node root;
    CALICODB_TRY(tree->acquire(root_id.page_id, false, root));
    if (root.header.cell_count != 0) {
        return Status::io_error("table could not be emptied");
    }
    auto *root_state = m_tables.get(Id::root());
    CALICODB_TRY(root_state->tree->erase(name));
    tree->upgrade(root);
    CALICODB_TRY(tree->destroy(std::move(root)));
    return Status::ok();
}

#undef SET_STATUS

} // namespace calicodb

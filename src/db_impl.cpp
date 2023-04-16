// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "logging.h"

namespace calicodb
{

static auto table_impl(const Table &table) -> const TableImpl &
{
    return reinterpret_cast<const TableImpl &>(table);
}

static auto table_impl(Table &table) -> TableImpl &
{
    return reinterpret_cast<TableImpl &>(table);
}

static constexpr auto encode_page_size(std::size_t page_size) -> U16
{
    return page_size < kMaxPageSize ? static_cast<U16>(page_size) : 0;
}

static constexpr auto decode_page_size(unsigned header_page_size) -> U32
{
    return header_page_size > 0 ? header_page_size : kMaxPageSize;
}

static auto unrecognized_txn(unsigned have_txn, unsigned want_txn)
{
    std::string message;
    if (want_txn) {
        append_fmt_string(
            message,
            "unrecognized txn number %u (current txn is %u)",
            have_txn,
            want_txn);
    } else {
        message = "transaction has not been started";
    }
    return Status::invalid_argument(message);
}

Table::~Table() = default;

TableImpl::TableImpl(std::string name, TableState &state, Id table_id)
    : m_name(std::move(name)),
      m_state(&state),
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

auto DBImpl::open(const Options &sanitized) -> Status
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
        File *file;
        char buffer[FileHeader::kSize];
        // TODO: Rather than checking if the file exists, just try to open it without Env::kCreate set.
        //       If it fails, then the DB does not exist and new_file() can be retried with Env::kCreate
        //       to create the file. If it does exist, use that file handle to read the header then pass
        //       it to the pager. It doesn't need to be closed and opened again.
        CALICODB_TRY(m_env->new_file(m_db_filename, Env::kReadWrite, file));
        auto s = file->read_exact(0, sizeof(buffer), buffer);
        delete file;
        if (!s.is_ok()) {
            return s;
        }
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
    CALICODB_TRY(Pager::open(pager_param, m_pager));

    if (db_exists) {
        m_pager->load_state(header);
    } else {
        logv(m_log, "setting up a new database");

        // Create the root table tree manually.
        CALICODB_TRY(Tree::create(*m_pager, Id::root(), nullptr));
    }

    // Create the root and default table handles.
    CALICODB_TRY(do_create_table(TableOptions(), kRootTableName, m_root));
    CALICODB_TRY(do_create_table(TableOptions(), kDefaultTableName, m_default));

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
        logv(m_log, "ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CALICODB_TRY(checkpoint_if_needed(true));
        m_pager->purge_all_pages();
        CALICODB_TRY(load_file_header());
    } else {
        // Write the initial file header containing the page size.
        auto root = m_pager->acquire_root();
        m_pager->upgrade(root);
        header.write(root.data());
        m_pager->release(std::move(root));

        // Commit the initial transaction. Since the WAL is not enabled, this will write
        // to the DB file and call fsync(). The dirty page set should include the root
        // page, the pointer map page on page 2, and the root of the default table.
        CALICODB_TRY(m_pager->commit_txn());
    }
    CALICODB_TRY(status());
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
    if (m_state.use_wal) {
        if (m_pager->mode() != Pager::kOpen) {
            if (auto s = m_pager->rollback_txn(); !s.is_ok()) {
                logv(m_log, "failed to revert uncommitted transaction: %s", s.to_string().c_str());
            }
        }
        if (m_pager->mode() == Pager::kOpen) {
            // If there was an error and rollback_txn() was able to fix it, then we can checkpoint
            // here. Otherwise, the call to Wal::close() below will not delete the WAL, and recovery
            // will be attempted next time DB::open() is called.
            if (const auto s = checkpoint_if_needed(true); !s.is_ok()) {
                logv(m_log, "failed to checkpoint database: %s", s.to_string().c_str());
            }
        }
        if (const auto s = Wal::close(m_wal); !s.is_ok()) {
            logv(m_log, "failed to close WAL: %s", s.to_string().c_str());
        }
    }

    delete m_default;
    delete m_root;
    delete m_pager;
    delete m_wal;

    if (m_owns_log) {
        delete m_log;
    }
    if (m_owns_env) {
        delete m_env;
    }
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
                append_fmt_string(
                    buffer,
                    "Name          Value\n"
                    "-------------------\n"
                    "Pager I/O(MB) %8.4f/%8.4f\n"
                    "WAL I/O(MB)   %8.4f/%8.4f\n"
                    "Cache hits    %ld\n"
                    "Cache misses  %ld\n",
                    static_cast<double>(m_pager->statistics().bytes_read) / 1048576.0,
                    static_cast<double>(m_pager->statistics().bytes_written) / 1048576.0,
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
                        const auto n = append_fmt_string(
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
    const auto &state = table_impl(table).state();
    auto *cursor = CursorInternal::make_cursor(*state.tree);
    if (m_pager->mode() == Pager::kError) {
        CALICODB_EXPECT_FALSE(m_state.status.is_ok());
        CursorInternal::invalidate(*cursor, m_state.status);
    }
    return cursor;
}

auto DBImpl::get(const Table &table, const Slice &key, std::string *value) const -> Status
{
    if (m_pager->mode() == Pager::kError) {
        return status();
    }
    return table_impl(table).state().tree->get(key, value);
}

auto DBImpl::put(Table &table, const Slice &key, const Slice &value) -> Status
{
    bool implicit_txn;
    CALICODB_TRY(ensure_txn_started(implicit_txn));
    CALICODB_TRY(do_put(table, key, value));
    return ensure_txn_finished(implicit_txn);
}

auto DBImpl::erase(Table &table, const Slice &key) -> Status
{
    bool implicit_txn;
    CALICODB_TRY(ensure_txn_started(implicit_txn));
    CALICODB_TRY(do_erase(table, key));
    return ensure_txn_finished(implicit_txn);
}

auto DBImpl::do_put(Table &table, const Slice &key, const Slice &value) -> Status
{
    auto &state = table_impl(table).state();
    if (!state.write) {
        return Status::invalid_argument("table is not writable");
    }
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }
    return state.tree->put(key, value, nullptr);
}

auto DBImpl::do_erase(Table &table, const Slice &key) -> Status
{
    auto &state = table_impl(table).state();
    if (!state.write) {
        return Status::invalid_argument("table is not writable");
    }
    return state.tree->erase(key);
}

auto DBImpl::vacuum() -> Status
{
    bool implicit_txn;
    CALICODB_TRY(ensure_txn_started(implicit_txn));
    CALICODB_TRY(do_vacuum());
    return ensure_txn_finished(implicit_txn);
}

auto DBImpl::do_vacuum() -> Status
{
    std::vector<std::string> table_names;
    std::vector<LogicalPageId> table_roots;
    CALICODB_TRY(get_table_info(table_names, &table_roots));

    Id target(m_pager->page_count());
    auto &state = table_impl(*m_root).state();
    auto *tree = state.tree;

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
        CALICODB_TRY(m_pager->set_status(
            put(*m_root, table_names[i], logical_id)));
    }
    m_pager->set_page_count(target.value);
    invalidate_live_cursors();

    logv(m_log, "vacuumed %llu pages", original.value - target.value);
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

auto DBImpl::begin_txn(const TxnOptions &) -> unsigned
{
    return m_txn += m_pager->begin_txn();
}

auto DBImpl::rollback_txn(unsigned txn) -> Status
{
    if (txn != m_txn || m_pager->mode() == Pager::kOpen) {
        return unrecognized_txn(txn, m_txn);
    }
    auto s = m_pager->rollback_txn();
    if (s.is_ok()) {
        invalidate_live_cursors();
        s = load_file_header();
    }
    return s;
}

auto DBImpl::commit_txn(unsigned txn) -> Status
{
    if (txn != m_txn || m_pager->mode() == Pager::kOpen) {
        return unrecognized_txn(txn, m_txn);
    }
    CALICODB_TRY(m_pager->commit_txn());

    if (m_sync) {
        // Failure to sync the WAL requires a rollback. Make sure the pager knows to
        // skip the checkpoint below.
        m_pager->set_status(
            m_wal->sync());
    }
    CALICODB_TRY(checkpoint_if_needed());
    return status();
}

auto DBImpl::checkpoint_if_needed(bool force) -> Status
{
    if (force || m_wal->needs_checkpoint()) {
        return m_pager->checkpoint();
    }
    return Status::ok();
}

auto DBImpl::load_file_header() -> Status
{
    auto root = m_pager->acquire_root();

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
    CALICODB_TRY(status());
    return get_table_info(out, nullptr);
}

auto DBImpl::create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    bool implicit_txn;
    CALICODB_TRY(ensure_txn_started(implicit_txn));
    CALICODB_TRY(do_create_table(options, name, out));
    return ensure_txn_finished(implicit_txn);
}

auto DBImpl::do_create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
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
        const auto &state = table_impl(*m_root).state();
        s = state.tree->get(name, &value);
        if (s.is_ok()) {
            s = decode_logical_id(value, &root_id);
        } else if (s.is_not_found()) {
            s = construct_new_table(name, root_id);
        }
    }

    if (s.is_ok()) {
        auto *state = m_tables.get(root_id.table_id);
        if (state == nullptr) {
            m_tables.add(root_id);
            state = m_tables.get(root_id.table_id);
        }
        CALICODB_EXPECT_NE(state, nullptr);

        if (state->open) {
            return Status::invalid_argument("table is already open");
        }
        state->tree = new Tree(*m_pager, root_id.page_id, &state->stats);
        state->write = options.mode == AccessMode::kReadWrite;
        state->open = true;
        out = new TableImpl(name, *state, root_id.table_id);
    }
    return Status::ok();
}

auto DBImpl::close_table(Table *&table) -> void
{
    if (table == nullptr || table == default_table()) {
        return;
    }
    auto &state = table_impl(*table).state();

    delete state.tree;
    state.tree = nullptr;
    state.write = false;
    state.open = false;
    delete table;
}

auto DBImpl::drop_table(Table *&table) -> Status
{
    bool implicit_txn;
    CALICODB_TRY(ensure_txn_started(implicit_txn));
    CALICODB_TRY(do_drop_table(table));
    return ensure_txn_finished(implicit_txn);
}

auto DBImpl::do_drop_table(Table *&table) -> Status
{
    if (table == nullptr) {
        return Status::ok();
    } else if (table == default_table()) {
        return Status::invalid_argument("cannot drop default table");
    }
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

    auto &impl = table_impl(*table);
    s = remove_empty_table(table->name(), impl.state());

    m_tables.erase(impl.id());
    delete table;
    table = nullptr;
    return s;
}

auto DBImpl::construct_new_table(const Slice &name, LogicalPageId &root_id) -> Status
{
    // Find the first available table ID.
    auto table_id = Id::root();
    for (const auto *state : m_tables) {
        if (state == nullptr) {
            break;
        }
        ++table_id.value;
    }
    // Set the table ID manually, let the tree fill in the root page ID.
    root_id.table_id = table_id;
    CALICODB_TRY(Tree::create(*m_pager, table_id, &root_id.page_id));

    char payload[LogicalPageId::kSize];
    encode_logical_id(root_id, payload);

    // Write an entry for the new table in the root table. This will not increase the
    // record count for the database.
    auto &root_state = table_impl(*m_root).state();
    return root_state.tree->put(name, Slice(payload, LogicalPageId::kSize));
}

auto DBImpl::remove_empty_table(const std::string &name, TableState &state) -> Status
{
    auto &[root_id, stats, tree, write_flag, open_flag] = state;
    CALICODB_EXPECT_FALSE(root_id.table_id.is_root());

    Node root;
    CALICODB_TRY(tree->acquire(root_id.page_id, false, root));
    if (root.header.cell_count != 0) {
        return Status::io_error("table could not be emptied");
    }
    auto &root_state = table_impl(*m_root).state();
    CALICODB_TRY(root_state.tree->erase(name));
    tree->upgrade(root);
    return tree->destroy(std::move(root));
}

auto DBImpl::ensure_txn_started(bool &implicit_txn) -> Status
{
    implicit_txn = m_pager->mode() == Pager::kOpen;
    if (implicit_txn) {
        (void)begin_txn(TxnOptions());
    }
    return status();
}

auto DBImpl::ensure_txn_finished(bool implicit_txn) -> Status
{
    if (implicit_txn) {
        if (m_pager->mode() == Pager::kError) {
            return rollback_txn(m_txn);
        } else {
            return commit_txn(m_txn);
        }
    }
    return status();
}

auto DBImpl::invalidate_live_cursors() -> void
{
    for (auto *state : m_tables) {
        if (state && state->tree) {
            CALICODB_EXPECT_TRUE(state->open);
            state->tree->inform_cursors();
        }
    }
}

} // namespace calicodb

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "calicodb/env.h"
#include "crc.h"
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

static auto check_header_crc(const FileHeader &header) -> bool
{
    return crc32c::Unmask(header.header_crc) == header.compute_crc();
}

static auto encode_page_size(std::size_t page_size) -> U16
{
    return page_size < kMaxPageSize ? static_cast<U16>(page_size) : 0;
}

static auto decode_page_size(unsigned header_page_size) -> std::size_t
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

auto setup_db(const std::string &filename, Env &env, Options &options, FileHeader &header) -> Status
{
    CALICODB_EXPECT_GE(options.page_size, kMinPageSize);
    CALICODB_EXPECT_LE(options.page_size, kMaxPageSize);
    CALICODB_EXPECT_TRUE(is_power_of_two(options.page_size));

    if (env.file_exists(filename)) {
        char buffer[FileHeader::kSize];
        File *reader;

        CALICODB_TRY(env.new_file(filename, reader));
        CALICODB_TRY(reader->read_exact(0, sizeof(buffer), buffer));
        delete reader;

        header.read(buffer);
        if (header.magic_code != FileHeader::kMagicCode) {
            return Status::invalid_argument("file is not a CalicoDB database");
        }
        if (!check_header_crc(header)) {
            return Status::corruption("database is corrupted");
        }
    } else {
        header.page_size = encode_page_size(options.page_size);
    }
    if (options.cache_size < kMinFrameCount * decode_page_size(header.page_size)) {
        options.cache_size = kMinFrameCount * decode_page_size(header.page_size);
    }
    return Status::ok();
}

auto DBImpl::open(Options sanitized) -> Status
{
    const auto db_exists = m_env->file_exists(m_db_filename);
    if (db_exists) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument("database already exists");
        }
    } else if (!sanitized.create_if_missing) {
        return Status::invalid_argument("database does not exist");
    }
    FileHeader state;
    CALICODB_TRY(setup_db(m_db_filename, *m_env, sanitized, state));
    const auto page_size = decode_page_size(state.page_size);

    m_state.ckpt_number = state.ckpt_number;
    m_state.record_count = state.record_count;
    m_state.freelist_head = state.freelist_head;
    m_state.max_page_id.value = state.page_count;

    CALICODB_TRY(Wal::open(
        {
            m_wal_filename,
            page_size,
            m_env,
        },
        m_wal));

    CALICODB_TRY(Pager::open(
        {
            m_db_filename,
            m_env,
            m_wal,
            m_log,
            &m_state,
            sanitized.cache_size / page_size,
            page_size,
        },
        &m_pager));
    m_pager->load_state(state);

    if (!db_exists) {
        m_log->logv("setting up a new database");
        CALICODB_TRY(m_env->sync_directory(split_path(m_db_filename).first));

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
        CALICODB_TRY(ensure_consistency());
        CALICODB_TRY(load_file_header());
    } else {
        // Write the initial file header.
        Page db_root;
        CALICODB_TRY(m_pager->acquire(Id::root(), db_root));
        m_pager->upgrade(db_root);
        state.page_count = static_cast<U32>(m_pager->page_count());
        state.header_crc = crc32c::Mask(state.compute_crc());
        state.write(db_root.data());
        m_pager->release(std::move(db_root));
        CALICODB_TRY(m_pager->flush());
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
      m_log_filename(sanitized.info_log == nullptr
                         ? m_db_filename + kDefaultLogSuffix
                         : ""),
      m_owns_env(options.env == nullptr),
      m_owns_log(options.info_log == nullptr)
{
}

DBImpl::~DBImpl()
{
    if (m_state.use_wal && m_state.status.is_ok()) {
        if (const auto s = m_pager->checkpoint(); !s.is_ok()) {
            m_log->logv("failed to reset wal: %s", s.to_string().c_str());
        }
        if (const auto s = ensure_consistency(); !s.is_ok()) {
            m_log->logv("failed to ensure consistency: %s", s.to_string().c_str());
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
        m_state.record_count += !record_exists;
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
        --m_state.record_count;
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
    // Make sure the vacuum updates are in the WAL. If this succeeds, we should
    // be able to reapply the whole vacuum operation if the truncation fails.
    // The recovery routine should truncate the file to match the header page
    // count if necessary.
    //    CALICODB_TRY(m_wal->synchronize(true));
    CALICODB_TRY(m_pager->truncate(target.value));

    m_log->logv("vacuumed %llu pages", original.value - target.value);
    return m_pager->flush();
}

auto DBImpl::ensure_consistency() -> Status
{
    CALICODB_TRY(m_state.status);
    m_state.use_wal = false;
    CALICODB_TRY(recovery_phase_1());
    CALICODB_TRY(recovery_phase_2());
    m_state.use_wal = true;
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

    if (m_state.batch_size > 0) {
        m_state.batch_size = 0;
        m_state.max_page_id = Id(m_pager->page_count());
        if (auto s = do_commit(); !s.is_ok()) {
            SET_STATUS(s);
            return s;
        }
    }
    return Status::ok();
}

auto DBImpl::do_commit() -> Status
{
    Page db_root;
    CALICODB_TRY(m_pager->acquire(Id::root(), db_root));
    m_pager->upgrade(db_root);

    FileHeader header;
    header.read(db_root.data());
    header.page_count = static_cast<U32>(m_pager->page_count());
    header.freelist_head = m_state.freelist_head;
    header.magic_code = FileHeader::kMagicCode;
    //    header.commit_lsn = m_wal->current_lsn();
    //    m_state.commit_lsn = m_wal->current_lsn();
    header.record_count = m_state.record_count;
    header.header_crc = crc32c::Mask(header.compute_crc());
    header.write(db_root.data());
    m_pager->release(std::move(db_root));

    CALICODB_TRY(m_wal->commit());

    // This call just performs some cleanup.
    auto s = m_pager->commit();
    if (!s.is_ok()) {
        SET_STATUS(s);
    }
    return Status::ok();
}

auto DBImpl::load_file_header() -> Status
{
    Page root;
    CALICODB_TRY(m_pager->acquire(Id::root(), root));

    FileHeader header;
    header.read(root.data());
    if (!check_header_crc(header)) {
        m_log->logv("file header crc mismatch (expected %u but computed %u)",
                    crc32c::Unmask(header.header_crc), header.compute_crc());
        return Status::corruption("crc mismatch");
    }
    // These values should be the same, provided that the WAL contents were correct.
    CALICODB_EXPECT_EQ(m_state.ckpt_number, header.ckpt_number);
    m_state.max_page_id.value = header.page_count;
    m_state.record_count = header.record_count;
    m_state.freelist_head = header.freelist_head;
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

// static auto apply_undo(Page &page, const ImageDescriptor &image)
//{
//     const auto data = image.image;
//     std::memcpy(page.data(), data.data(), data.size());
//     if (page.size() > data.size()) {
//         std::memset(page.data() + data.size(), 0, page.size() - data.size());
//     }
// }
//
// static auto apply_redo(Page &page, const DeltaDescriptor &delta)
//{
//     for (auto [offset, data] : delta.deltas) {
//         std::memcpy(page.data() + offset, data.data(), data.size());
//     }
// }
//
// template <class Descriptor, class Callback>
// static auto with_page(Pager &pager, const Descriptor &descriptor, const Callback &callback)
//{
//     Page page;
//     CALICODB_TRY(pager.acquire(descriptor.page_id, page));
//
//     callback(page);
//     pager.release(std::move(page));
//     return Status::ok();
// }
//
// static auto is_commit(const DeltaDescriptor &deltas)
//{
//     return deltas.page_id.is_root() && deltas.deltas.size() == 1 && deltas.deltas.front().offset == 0 &&
//            deltas.deltas.front().data.size() == FileHeader::kSize + Lsn::kSize;
// }

auto DBImpl::recovery_phase_1() -> Status
{
    //    const auto &set = m_wal->m_segments;
    //
    //    if (set.empty()) {
    //        return Status::ok();
    //    }
    //
    //    // It can be difficult to determine if there is already an image record for a given page
    //    // in the WAL, due to the way the vacuum operation works. This set is used to ignore
    //    // duplicate image records within each transaction.
    //    std::unordered_set<Id, Id::Hash> duplicates;
    //    std::string tail(wal_block_size(m_pager->page_size()), '\0');
    //    std::unique_ptr<File> file;
    //    auto itr = begin(set);
    //    auto commit_lsn = m_state.commit_lsn;
    //    auto commit_itr = itr;
    //    Lsn last_lsn;
    //
    //    const auto translate_status = [&itr, &set, this](auto s, Lsn lsn) {
    //        CALICODB_EXPECT_FALSE(s.is_ok());
    //        if (s.is_corruption()) {
    //            // Allow corruption/incomplete records on the last segment, past the
    //            // most-recent successful commit.
    //            if (itr == prev(end(set)) && m_state.commit_lsn <= lsn) {
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
    //            if (is_commit(deltas)) {
    //                commit_lsn = deltas.lsn;
    //                commit_itr = itr;
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
    //            CALICODB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
    //            return Status::not_found("finished");
    //        }
    //        return Status::ok();
    //    };
    //
    //    const auto undo = [&](const auto &payload) {
    //        if (last_lsn <= m_state.commit_lsn) {
    //            return Status::ok();
    //        }
    //        const auto decoded = decode_payload(payload);
    //        if (std::holds_alternative<DeltaDescriptor>(decoded)) {
    //            if (is_commit(std::get<DeltaDescriptor>(decoded))) {
    //                duplicates.clear();
    //            }
    //        } else if (std::holds_alternative<ImageDescriptor>(decoded)) {
    //            const auto image = std::get<ImageDescriptor>(decoded);
    //            return with_page(*m_pager, image, [&](auto &page) {
    //                if (!duplicates.insert(image.page_id).second) {
    //                    return;
    //                }
    //                const auto page_lsn = read_page_lsn(page);
    //                if (page_lsn.is_null() || image.lsn < page_lsn) {
    //                    m_pager->upgrade(page);
    //                    apply_undo(page, image);
    //                }
    //            });
    //        } else if (std::holds_alternative<std::monostate>(decoded)) {
    //            CALICODB_TRY(translate_status(Status::corruption("wal is corrupted"), last_lsn));
    //            return Status::not_found("finished");
    //        }
    //        return Status::ok();
    //    };
    //
    //    const auto roll = [&](const auto &action) {
    //        CALICODB_TRY(open_wal_reader(itr->first, file));
    //        WalReader reader {*file, tail};
    //
    //        for (;;) {
    //            std::string reader_data;
    //            auto s = reader.read(reader_data);
    //            Slice payload(reader_data);
    //
    //            if (s.is_not_found()) {
    //                break;
    //            } else if (!s.is_ok()) {
    //                CALICODB_TRY(translate_status(s, last_lsn));
    //                return Status::ok();
    //            }
    //
    //            last_lsn = extract_payload_lsn(payload);
    //            s = action(payload);
    //
    //            if (s.is_not_found()) {
    //                break;
    //            } else if (!s.is_ok()) {
    //                return s;
    //            }
    //        }
    //        return Status::ok();
    //    };
    //
    //    // Roll forward, applying missing updates until we reach the end. The final
    //    // segment may contain a partial/corrupted record.
    //    for (; itr != end(set); ++itr) {
    //        CALICODB_TRY(roll(redo));
    //    }
    //
    //    // Didn't make it to the end of the WAL.
    //    if (itr != end(set)) {
    //        return Status::corruption("wal could not be read to the end");
    //    }
    //
    //    if (last_lsn == commit_lsn) {
    //        if (m_state.commit_lsn <= commit_lsn) {
    //            m_state.commit_lsn = commit_lsn;
    //            return Status::ok();
    //        } else {
    //            return Status::corruption("missing commit record");
    //        }
    //    }
    //    m_state.commit_lsn = commit_lsn;
    //
    //    // Roll backward, reverting updates until we reach the most-recent commit. We
    //    // are able to read the log forward, since the full images are disjoint.
    //    // Again, the last segment we read may contain a partial/corrupted record.
    //    itr = commit_itr;
    //    for (; itr != end(set); ++itr) {
    //        CALICODB_TRY(roll(undo));
    //    }
    return Status::ok();
}

auto DBImpl::recovery_phase_2() -> Status
{
    //    auto &set = m_wal->m_segments;
    //    // Pager needs the updated state to determine the page count.
    //    CALICODB_TRY(load_file_header());
    //
    //    m_wal->m_last_lsn = m_state.commit_lsn;
    //    m_wal->m_flushed_lsn = m_state.commit_lsn;
    //
    //    // Make sure all changes have made it to disk, then remove WAL segments from
    //    // the right.
    //    CALICODB_TRY(m_pager->flush());
    //    for (auto itr = rbegin(set); itr != rend(set); ++itr) {
    //        CALICODB_TRY(m_env->remove_file(encode_segment_name(m_wal->m_prefix, itr->first)));
    //    }
    //    set.clear();
    //
    //    // Make sure the file size matches the header page count, which should be
    //    // correct if we made it this far.
    //    CALICODB_TRY(m_pager->truncate(m_pager->page_count()));
    //    return m_pager->commit();
    return Status::ok();
}

// auto DBImpl::open_wal_reader(Id segment, std::unique_ptr<File> &out) -> Status
//{
//     File *file;
//     const auto name = encode_segment_name(m_wal_prefix, segment);
//     CALICODB_TRY(m_env->new_file(name, file));
//     out.reset(file);
//     return Status::ok();
// }

#undef SET_STATUS

} // namespace calicodb

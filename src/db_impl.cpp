// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "logging.h"
#include "scope_guard.h"
#include "txn_impl.h"

namespace calicodb
{

static constexpr auto encode_page_size(std::size_t page_size) -> U16
{
    return page_size < kMaxPageSize ? static_cast<U16>(page_size) : 0;
}

static constexpr auto decode_page_size(unsigned header_page_size) -> U32
{
    return header_page_size > 0 ? header_page_size : kMaxPageSize;
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
    } else if (!sanitized.create_if_missing) {
        return Status::invalid_argument("database does not exist");
    }

    // Open the database file.
    File *file;
    CALICODB_TRY(m_env->new_file(
        m_db_filename,
        Env::kCreate | Env::kReadWrite,
        file));

    ScopeGuard guard = [file] {
        delete file;
    };

    if (db_exists) {
        char buffer[FileHeader::kSize];
        CALICODB_TRY(file->read_exact(0, sizeof(buffer), buffer));
        if (!header.read(buffer)) {
            return Status::invalid_argument(
                "file \"" + m_db_filename + "\" is not a CalicoDB database");
        }
    } else {
        header.page_size = encode_page_size(sanitized.page_size);
    }

    const auto page_size = decode_page_size(header.page_size);
    const auto cache_size = std::max(sanitized.cache_size, kMinFrameCount * page_size);

    const Pager::Parameters pager_param = {
        m_db_filename,
        m_wal_filename,
        file,
        m_env,
        m_log,
        &m_state,
        cache_size / page_size,
        page_size,
    };
    CALICODB_TRY(Pager::open(pager_param, m_pager));

    if (db_exists) {
        logv(m_log, "ensuring consistency of an existing database");

        // This should be a no-op if the database closed normally last time.
        CALICODB_TRY(m_pager->checkpoint());
        m_pager->purge_all_pages();

    } else {
        logv(m_log, "setting up a new database");
        // Write the root database page contents to this buffer. After this buffer is
        // written to the start of the database file, the database is considered
        // initialized.
        std::string initial(page_size, '\0');
        header.page_count = 1;
        header.write(initial.data());

        NodeHeader root_hdr;
        root_hdr.is_external = true;
        root_hdr.cell_start = page_size;
        root_hdr.write(initial.data() + FileHeader::kSize);

        CALICODB_TRY(m_pager->m_file->write(0, initial));
        CALICODB_TRY(m_pager->m_file->sync());
    }
    m_state.use_wal = true;
    std::move(guard).cancel();
    return Status::ok();
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_env(sanitized.env),
      m_log(sanitized.info_log),
      m_db_filename(std::move(filename)),
      m_wal_filename(sanitized.wal_filename),
      m_owns_env(options.env == nullptr),
      m_owns_log(options.info_log == nullptr),
      m_sync(options.sync)
{
}

DBImpl::~DBImpl()
{
    if (m_state.use_wal) {
        if (m_pager->mode() != Pager::kOpen) {
            m_pager->finish();
        }
        // If someone else is still reading or writing the database, this call
        // will return a Status::busy(). The last process to access the database
        // will run the checkpoint, or if that fails, it will be run on the next
        // startup.
        auto s = m_pager->checkpoint();
        if (!s.is_ok()) {
            logv(m_log, "failed to checkpoint database: %s", s.to_string().c_str());
        }

        s = m_pager->close();
        if (!s.is_ok()) {
            logv(m_log, "failed to close pager: %s", s.to_string().c_str());
        }
    }

    delete m_pager;

    if (m_owns_log) {
        delete m_log;
    }
    if (m_owns_env) {
        delete m_env;
    }
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
    const auto wal_name = impl->m_wal_filename;
    delete db;

    auto *env = options.env;
    if (env == nullptr) {
        env = Env::default_env();
    }

    (void)env->remove_file(db_name);
    (void)env->remove_file(wal_name);

    if (env != options.env) {
        delete env;
    }

    return Status::ok();
}

auto DBImpl::get_property(const Slice &name, std::string *out) const -> bool
{
    if (out) {
        *out = nullptr;
    }
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
                    static_cast<double>(m_pager->statistics().bytes_read) / 1'048'576.0,
                    static_cast<double>(m_pager->statistics().bytes_written) / 1'048'576.0,
                    static_cast<double>(m_pager->wal_statistics().bytes_read) / 1'048'576.0,
                    static_cast<double>(m_pager->wal_statistics().bytes_written) / 1'048'576.0,
                    m_pager->hits(),
                    m_pager->misses());
                out->append(buffer);
            }
            return true;
        }
    }
    return false;
}

auto DBImpl::start(bool write, Txn *&out) -> Status
{
    if (write) {
        CALICODB_TRY(m_pager->checkpoint()); // TODO: Find somewhere better to put this, maybe even expose a checkpoint() method on DB
    }
    CALICODB_TRY(m_pager->begin(write));
    out = new TxnImpl(*m_pager, m_state);
    return Status::ok();
}

auto DBImpl::finish(Txn *&txn) -> void
{
    delete txn;
    txn = nullptr;
}

auto DBImpl::TEST_pager() const -> const Pager &
{
    return *m_pager;
}

auto DBImpl::TEST_state() const -> const DBState &
{
    return m_state;
}

} // namespace calicodb

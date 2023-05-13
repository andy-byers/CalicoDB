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

auto DBImpl::open(const Options &sanitized) -> Status
{
    File *file = nullptr;
    auto exists = false;
    ScopeGuard guard = [&file, this] {
        if (m_pager == nullptr) {
            delete file;
        }
    };

    auto s = m_env->new_file(m_db_filename, Env::kReadWrite, file);
    if (s.is_ok()) {
        exists = true;
    } else if (s.is_io_error()) {
        if (!sanitized.create_if_missing) {
            return Status::invalid_argument(
                "database \"" + m_db_filename + "\" does not exist");
        }
        s = m_env->new_file(m_db_filename, Env::kCreate, file);
    }
    if (!s.is_ok()) {
        return s;
    }
    const auto cache_size = std::max(
        sanitized.cache_size, kMinFrameCount * kPageSize);

    const Pager::Parameters pager_param = {
        m_db_filename.c_str(),
        m_wal_filename.c_str(),
        file,
        m_env,
        m_log,
        &m_state,
        m_busy,
        (cache_size + kPageSize - 1) / kPageSize,
        sanitized.sync,
    };
    CALICODB_TRY(Pager::open(pager_param, m_pager));

    if (exists) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument(
                "database \"" + m_db_filename + "\" already exists");
        }
        if (m_env->file_exists(m_wal_filename)) {
            s = m_pager->checkpoint(false);
        }
    } else {
        logv(m_log, "setting up a new database");
        if (m_env->remove_file(m_wal_filename).is_ok()) {
            logv(m_log, R"(removed old WAL file at "%s")", m_wal_filename.c_str());
        }
    }
    std::move(guard).cancel();
    return s;
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_env(sanitized.env),
      m_log(sanitized.info_log),
      m_busy(sanitized.busy),
      m_db_filename(std::move(filename)),
      m_wal_filename(sanitized.wal_filename),
      m_owns_log(options.info_log == nullptr)
{
}

DBImpl::~DBImpl()
{
    if (m_pager) {
        const auto s = m_pager->close();
        if (!s.is_ok()) {
            logv(m_log, "failed to close pager: %s", s.to_string().c_str());
        }
    }
    delete m_pager;

    if (m_owns_log) {
        delete m_log;
    }
}

auto DBImpl::destroy(const Options &options, const std::string &filename) -> Status
{
    auto copy = options;
    copy.error_if_exists = false;
    copy.create_if_missing = false;

    DB *db;
    Txn *txn;
    Status s;

    // Determine the WAL filename, and make sure `filename` refers to a CalicoDB
    // database. The file identifier is not checked until a transaction is started.
    std::string wal_name;
    if ((s = DB::open(copy, filename, db)).is_ok()) {
        wal_name = db_impl(db)->m_wal_filename;
        if ((s = db->new_txn(false, txn)).is_ok()) {
            delete txn;
        }
        delete db;
    }

    // Remove the database files from the Env.
    Status t;
    if (s.is_ok()) {
        auto *env = options.env;
        if (env == nullptr) {
            env = Env::default_env();
        }
        s = env->remove_file(filename);
        if (env->file_exists(wal_name)) {
            // Delete the WAL file if it wasn't properly cleaned up when the database
            // was closed above. Under normal conditions, this branch is not hit.
            t = env->remove_file(wal_name);
        }
    }
    return s.is_ok() ? t : s;
}

auto DBImpl::get_property(const Slice &name, std::string *out) const -> bool
{
    if (out) {
        out->clear();
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

static auto already_running_error(bool write) -> Status
{
    std::string message("a transaction (read");
    message.append(write ? "-write" : "only");
    message.append(") is running");
    return Status::not_supported(message);
}

auto DBImpl::checkpoint(bool reset) -> Status
{
    if (m_txn) {
        return already_running_error(m_txn->m_write);
    }
    return m_pager->checkpoint(reset);
}

auto DBImpl::new_txn(bool write, Txn *&out) -> Status
{
    out = nullptr;
    if (m_txn) {
        return already_running_error(m_txn->m_write);
    }
    ScopeGuard guard = [this] {
        m_pager->finish();
    };

    // Forward error statuses. If an error is set at this point, then something
    // has gone very wrong.
    CALICODB_TRY(m_state.status);
    CALICODB_TRY(m_pager->start_reader());
    if (write) {
        CALICODB_TRY(m_pager->start_writer());
    }
    m_txn = new TxnImpl(*m_pager, m_state.status, write);
    m_txn->m_backref = &m_txn;
    out = m_txn;
    std::move(guard).cancel();
    return Status::ok();
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

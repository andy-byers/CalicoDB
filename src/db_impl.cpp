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
        if (file) {
            file->file_unlock();
            if (m_pager == nullptr) {
                delete file;
            }
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
        CALICODB_TRY(m_env->new_file(m_db_filename, Env::kCreate, file));
    } else {
        return s;
    }
    CALICODB_TRY(file->file_lock(kLockShared));

    FileHeader header;
    if (exists) {
        Slice read;
        char buffer[kPageSize];
        CALICODB_TRY(file->read(0, kPageSize, buffer, &read));
        if (read.size() == kPageSize) {
            exists = header.read(buffer);
        } else if (read.is_empty()) {
            exists = false;
        } else {
            std::string message("root page is incomplete: read ");
            append_fmt_string(message, "%zu/%zu bytes", read.size(), kPageSize);
            return Status::corruption(message);
        }
    }

    const auto cache_size = std::max(
        sanitized.cache_size, kMinFrameCount * kPageSize);

    const Pager::Parameters pager_param = {
        m_db_filename,
        m_wal_filename,
        file,
        m_env,
        m_log,
        &m_state,
        nullptr,
        (cache_size + kPageSize - 1) / kPageSize,
    };
    CALICODB_TRY(Pager::open(pager_param, m_pager));

    if (!exists) {
        logv(m_log, "setting up a new database");
        CALICODB_TRY(file->file_lock(kLockExclusive));
        m_pager->initialize_root();
        const auto *root = m_pager->m_bufmgr.root();
        CALICODB_TRY(m_pager->write_page_to_file(*root));
        CALICODB_TRY(file->sync());

    } else if (sanitized.error_if_exists) {
        return Status::invalid_argument(
            "database \"" + m_db_filename + "\" already exists");
    }
    std::move(guard).cancel();
    m_state.use_wal = true;
    file->file_unlock();
    return Status::ok();
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_env(sanitized.env),
      m_log(sanitized.info_log),
      m_busy(sanitized.busy),
      m_db_filename(std::move(filename)),
      m_wal_filename(sanitized.wal_filename),
      m_owns_env(options.env == nullptr),
      m_owns_log(options.info_log == nullptr),
      m_sync(options.sync)
{
}

DBImpl::~DBImpl()
{
    if (m_pager) {
        auto s = m_pager->close();
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
        return Status::invalid_argument(
            '"' + filename + "\" is not a CalicoDB database");
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

auto DBImpl::new_txn(bool write, Txn *&out) -> Status
{
    out = nullptr;
    if (m_txn) {
        std::string message("another transaction (read");
        message.append(m_txn->m_write ? "-write" : "only");
        message.append(") is already running");
        return Status::not_supported(message);
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

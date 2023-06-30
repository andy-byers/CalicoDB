// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "calicodb/env.h"
#include "logging.h"
#include "pager.h"
#include "tx_impl.h"

namespace calicodb
{

auto DBImpl::open(const Options &sanitized) -> Status
{
    auto s = m_env->new_file(m_db_filename, Env::kReadWrite, m_file);
    if (s.is_ok()) {
        if (sanitized.error_if_exists) {
            return Status::invalid_argument(
                "database \"" + m_db_filename + "\" already exists");
        }
    } else if (s.is_not_found()) {
        if (!sanitized.create_if_missing) {
            return Status::invalid_argument(
                "database \"" + m_db_filename + "\" does not exist");
        }
        // If there exists a file named m_wal_filename, then it must either be leftover from a
        // failed call to DB::destroy(), or it is an unrelated file that coincidentally has the
        // same name as this database's WAL file. Either way, we must get rid of it here,
        // otherwise we'll end up checkpointing it.
        s = m_env->remove_file(m_wal_filename);
        if (s.is_ok()) {
            log(m_log, R"(removed old WAL file "%s")", m_wal_filename.c_str());
        } else if (!s.is_not_found()) {
            return s;
        }
        log(m_log, R"(creating missing database "%s")", m_db_filename.c_str());
        s = m_env->new_file(m_db_filename, Env::kCreate, m_file);
    }
    if (s.is_ok()) {
        s = busy_wait(m_busy, [this] {
            // This lock is held for the entire lifetime of this DB.
            return m_file->file_lock(kFileShared);
        });
    }
    if (s.is_ok() && sanitized.lock_mode == Options::kLockExclusive) {
        s = m_file->file_lock(kFileExclusive);
    }
    if (!s.is_ok()) {
        return s;
    }
    const Pager::Parameters pager_param = {
        m_db_filename.c_str(),
        m_wal_filename.c_str(),
        m_file,
        m_env,
        m_log,
        &m_status,
        &m_stat,
        m_busy,
        (sanitized.cache_size + kPageSize - 1) / kPageSize,
        sanitized.sync_mode,
        sanitized.lock_mode,
    };
    // Pager::open() will open/create the WAL file. If a WAL file exists beforehand, then we
    // should attempt a checkpoint before we do anything else. If this is not the first
    // connection, then a checkpoint really isn't necessary, but it reduces the amount of
    // work needed when DB::checkpoint() is actually called. If this is actually the first
    // connection, then fsync() must be called on each file before it is used, to make sure
    // there isn't any data left in the kernel page cache.
    const auto needs_ckpt = m_env->file_exists(m_wal_filename);
    s = Pager::open(pager_param, m_pager);
    if (s.is_ok() && needs_ckpt) {
        s = m_pager->checkpoint(false);
        if (s.is_busy()) {
            s = Status::ok();
        }
    }
    return s;
}

DBImpl::DBImpl(const Options &options, const Options &sanitized, std::string filename)
    : m_scratch(new char[kPageSize * 2]),
      m_env(sanitized.env),
      m_log(sanitized.info_log),
      m_busy(sanitized.busy),
      m_db_filename(std::move(filename)),
      m_wal_filename(sanitized.wal_filename),
      m_owns_log(options.info_log == nullptr)
{
}

DBImpl::~DBImpl()
{
    delete m_pager;
    delete m_file;
    delete[] m_scratch;

    if (m_owns_log) {
        delete m_log;
    }
}

auto DBImpl::destroy(const Options &options, const std::string &filename) -> Status
{
    // Make sure `filename` refers to a CalicoDB database.
    DB *db;
    auto copy = options;
    copy.error_if_exists = false;
    copy.create_if_missing = false;
    auto s = DB::open(copy, filename, db);
    if (s.is_ok()) {
        // The file header is not checked until a transaction is started. Run a read
        // transaction, which will return with a non-OK status if `filename` is not a
        // valid database.
        s = db->view([](auto &) {
            return Status::ok();
        });
        delete db;
    }

    // Remove the database files from the Env.
    if (s.is_ok()) {
        auto *env = options.env;
        if (env == nullptr) {
            env = Env::default_env();
        }
        s = env->remove_file(filename);
        if (s.is_ok()) {
            log(options.info_log, R"(destroyed database file "%s")", filename.c_str());
        }

        // Destroy the WAL file, if it exists. If the DB was closed properly above, then neither
        // the WAL nor shm files should exist. This is to handle cases where that didn't happen.
        const auto wal_name = options.wal_filename.empty()
                                  ? filename + kDefaultWalSuffix
                                  : options.wal_filename;
        if (env->file_exists(wal_name)) {
            const auto t = env->remove_file(wal_name);
            if (t.is_ok()) {
                log(options.info_log, R"(destroyed WAL file "%s")", wal_name.c_str());
            }
        }
        const auto shm_name = filename + kDefaultShmSuffix;
        if (env->file_exists(shm_name)) {
            const auto t = env->remove_file(shm_name);
            if (t.is_ok()) {
                log(options.info_log, R"(destroyed shm file "%s")", shm_name.c_str());
            }
        }
    }
    return s;
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
            if (out == nullptr) {
                return true;
            }
            append_fmt_string(
                buffer,
                "Name               Value\n"
                "------------------------\n"
                "DB read(MB)   %10.4f\n"
                "DB write(MB)  %10.4f\n"
                "DB sync       %10llu\n"
                "WAL read(MB)  %10.4f\n"
                "WAL write(MB) %10.4f\n"
                "WAL sync      %10llu\n"
                "SMO count     %10llu\n"
                "Cache hit %%   %10.4f\n",
                static_cast<double>(m_stat.counters[Stat::kReadDB]) / 1'048'576.0,
                static_cast<double>(m_stat.counters[Stat::kWriteDB]) / 1'048'576.0,
                m_stat.counters[Stat::kSyncDB],
                static_cast<double>(m_stat.counters[Stat::kReadWal]) / 1'048'576.0,
                static_cast<double>(m_stat.counters[Stat::kWriteWal]) / 1'048'576.0,
                m_stat.counters[Stat::kSyncWal],
                m_stat.counters[Stat::kSMOCount],
                static_cast<double>(m_stat.counters[Stat::kCacheHits]) /
                    static_cast<double>(m_stat.counters[Stat::kCacheHits] +
                                        m_stat.counters[Stat::kCacheMisses]));
            out->append(buffer);
            return true;
        }
    }
    return false;
}

static auto already_running_error() -> Status
{
    return Status::not_supported("another Tx is live");
}

auto DBImpl::checkpoint(bool reset) -> Status
{
    if (m_tx) {
        return already_running_error();
    }
    log(m_log, "running%s checkpoint", reset ? " reset" : "");
    return m_pager->checkpoint(reset);
}

template <class TxType>
auto DBImpl::prepare_tx(bool write, TxType *&tx_out) const -> Status
{
    tx_out = nullptr;
    if (m_tx) {
        return already_running_error();
    }

    // Forward error statuses. If an error is set at this point, then something
    // has gone very wrong.
    auto s = m_status;
    if (s.is_ok() && m_auto_ckpt) {
        s = m_pager->auto_checkpoint(m_auto_ckpt);
        s = s.is_busy() ? Status::ok() : s;
    }
    if (!s.is_ok()) {
        return s;
    }
    s = m_pager->start_reader();
    if (s.is_ok() && write) {
        s = m_pager->start_writer();
    }
    if (s.is_ok()) {
        m_tx = new TxImpl(*m_pager, m_status, m_stat, m_scratch);
        m_tx->m_backref = &m_tx;
        tx_out = m_tx;
    } else {
        m_pager->finish();
    }
    return s;
}

auto DBImpl::new_tx(WriteTag, Tx *&tx_out) -> Status
{
    return prepare_tx(true, tx_out);
}

auto DBImpl::new_tx(const Tx *&tx_out) const -> Status
{
    return prepare_tx(false, tx_out);
}

auto DBImpl::TEST_pager() const -> const Pager &
{
    return *m_pager;
}

} // namespace calicodb

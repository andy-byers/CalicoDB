// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "alloc.h"
#include "calicodb/env.h"
#include "logging.h"
#include "pager.h"
#include "tx_impl.h"

namespace calicodb
{

namespace
{

auto already_running_error() -> Status
{
    return Status::not_supported("another transaction is running");
}

} // namespace

auto DBImpl::open(const Options &sanitized) -> Status
{
    auto s = m_env->new_file(m_db_filename.c_str(),
                             Env::kReadWrite,
                             m_file.ref());
    if (s.is_ok()) {
        if (sanitized.error_if_exists) {
            return StatusBuilder::invalid_argument(R"(database "%s" already exists)",
                                                   m_db_filename.c_str());
        }
    } else if (s.is_not_found()) {
        if (!sanitized.create_if_missing) {
            return StatusBuilder::invalid_argument(R"(database "%s" does not exist)",
                                                   m_db_filename.c_str());
        }
        // If there exists a file named m_wal_filename, then it must either be leftover from a
        // failed call to DB::destroy(), or it is an unrelated file that coincidentally has the
        // same name as this database's WAL file. Either way, we must get rid of it here,
        // otherwise we'll end up checkpointing it.
        s = m_env->remove_file(m_wal_filename.c_str());
        if (s.is_ok()) {
            log(m_log, R"(removed old WAL file "%s")", m_wal_filename.c_str());
        } else if (!s.is_not_found()) {
            return s;
        }
        log(m_log, R"(creating missing database "%s")", m_db_filename.c_str());
        s = m_env->new_file(m_db_filename.c_str(), Env::kCreate, m_file.ref());
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
        m_file.get(),
        m_env,
        m_log,
        &m_status,
        &m_stat,
        m_busy,
        (sanitized.cache_size + kPageSize - 1) / kPageSize,
        sanitized.sync_mode,
        sanitized.lock_mode,
        !sanitized.temp_database,
    };
    // Pager::open() will open/create the WAL file. If a WAL file exists beforehand, then we
    // should attempt a checkpoint before we do anything else. If this is not the first
    // connection, then a checkpoint really isn't necessary, but it reduces the amount of
    // work needed when DB::checkpoint() is actually called. If this is actually the first
    // connection, then fsync() must be called on each file before it is used, to make sure
    // there isn't any data left in the kernel page cache.
    const auto needs_ckpt = m_env->file_exists(m_wal_filename.c_str());
    s = Pager::open(pager_param, m_pager.ref());
    if (s.is_ok() && needs_ckpt) {
        s = m_pager->checkpoint(false);
        if (s.is_busy()) {
            s = Status::ok();
        }
    }
    return s;
}

DBImpl::DBImpl(Parameters param)
    : m_scratch(move(param.scratch)),
      m_env(param.sanitized.env),
      m_log(param.sanitized.info_log),
      m_busy(param.sanitized.busy),
      m_auto_ckpt(param.sanitized.auto_checkpoint),
      m_db_filename(move(param.db_name)),
      m_wal_filename(move(param.wal_name)),
      m_owns_log(param.sanitized.info_log == nullptr),
      m_owns_env(param.sanitized.temp_database)
{
}

DBImpl::~DBImpl()
{
    m_pager.reset();
    m_file.reset();

    if (m_owns_log) {
        delete m_log;
    }
    if (m_owns_env) {
        delete m_env;
    }
}

auto DBImpl::destroy(const Options &options, const char *filename) -> Status
{
    auto copy = options;
    copy.cache_size = 0;
    copy.error_if_exists = false;
    copy.create_if_missing = false;
    copy.lock_mode = Options::kLockExclusive;

    DB *db;
    auto s = DB::open(copy, filename, db);
    if (s.is_ok()) {
        // The file header is not checked until a transaction is started. Run a read
        // transaction, which will return with a non-OK status if `filename` is not a
        // valid database.
        s = db->run(ReadOptions(), [](auto &) {
            return Status::ok();
        });
        if (s.is_ok()) {
            auto *env = options.env;
            if (env == nullptr) {
                env = &Env::default_env();
            }
            // Remove the database file from disk. The WAL file should be cleaned up
            // automatically.
            s = env->remove_file(filename);

            // This DB doesn't use a shm file, since it was opened in exclusive locking
            // mode. shm files left by other connections must be removed manually.
            String path_buffer;
            if (append_strings(
                    path_buffer,
                    Slice(filename, std::strlen(filename)),
                    kDefaultShmSuffix)) {
                s = Status::no_memory();
            } else {
                auto t = env->remove_file(path_buffer.c_str());
                if (t.is_ok()) {
                    log(options.info_log, R"(removed leftover shm file "%s")",
                        path_buffer.c_str());
                } else if (s.is_ok() && !t.is_not_found()) {
                    s = t;
                }
            }
        }
        delete db;
    }
    return s;
}

auto DBImpl::get_property(const Slice &name, String *out) const -> Status
{
    if (out) {
        out->clear();
    }
    static constexpr char kBasePrefix[] = "calicodb.";
    if (name.starts_with(kBasePrefix)) {
        const auto prop = name.range(std::strlen(kBasePrefix));

        if (prop == "stats") {
            int rc = 0;
            if (out) {
                rc = append_format_string(
                    *out,
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
            }
            return rc ? Status::no_memory() : Status::ok();
        }
    }
    return Status::not_found();
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
        CALICODB_EXPECT_TRUE(m_status.is_ok());
        m_tx = new (std::nothrow) TxImpl(TxImpl::Parameters{
            &m_status,
            m_pager.get(),
            &m_stat,
            m_scratch.ptr(),
            write,
        });
        if (m_tx && m_tx->m_schema.cursor()) {
            m_tx->m_backref = &m_tx;
            // The Schema object sets the pager status to Status::no_memory() if it was unable to
            // acquire memory for its bucket cursor. m_tx will be cleaned up below in this case.
            s = m_status;
        } else {
            s = Status::no_memory();
        }
    }
    if (s.is_ok()) {
        tx_out = m_tx;
    } else if (m_tx) {
        delete m_tx;
        m_tx = nullptr;
    } else {
        m_pager->finish();
    }
    return s;
}

auto DBImpl::new_tx(const WriteOptions &, Tx *&tx_out) -> Status
{
    return prepare_tx(true, tx_out);
}

auto DBImpl::new_tx(const ReadOptions &, Tx *&tx_out) const -> Status
{
    return prepare_tx(false, tx_out);
}

auto DBImpl::TEST_pager() const -> Pager &
{
    return *m_pager;
}

} // namespace calicodb

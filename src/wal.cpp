// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal.h"
#include "encoding.h"
#include "env_posix.h"
#include "frames.h"
#include "hash_index.h"
#include "logging.h"

namespace calicodb
{

struct WalHeader {
    static constexpr std::size_t kSize = 32;
    static constexpr U32 kMagic = 0x87654321; // TODO: more-or-less arbitrary, but could make it spell something cool?
    static constexpr U32 kVersion = 0x01'000'000;

    U32 magic_code = kMagic;
    U32 version = kVersion;
    U32 page_size = 0;
    U32 commit = 0;
    U32 salt[2] = {};
    U32 cksum[2] = {};
};

struct CheckpointInfo {
    // Number of WAL frames that have been written back to the DB.
    std::size_t backfill = 0;

    // The code that handles checkpoints will set this field before attempting to
    // backfill any frames. "backfill_attempts" <= "backfill" is always true, as
    // is "backfill" <= "max_frame", where "max_frame" is the largest frame ID
    // contained in the WAL.
    std::size_t backfill_attempts = 0;
};

struct WalFrameHeader {
    static constexpr std::size_t kSize = 24;

    auto read(const char *data) -> void
    {
        pgno = get_u32(data);
        data += sizeof(U32);

        db_size = get_u32(data);
        data += sizeof(U32);

        for (auto *arr : {salt, cksum}) {
            arr[0] = get_u32(data);
            data += sizeof(U32);

            arr[1] = get_u32(data);
            data += sizeof(U32);
        }
    }

    U32 pgno = 0;

    // DB header page count after a commit (nonzero for commit frames, 0 for
    // all other frames).
    U32 db_size = 0;

    // Salts, copied from the WAL header. Must match what is in the header for the
    // frame to be considered valid.
    U32 salt[2] = {};

    U32 cksum[2] = {};
};

class WalImpl : public Wal
{
public:
    explicit WalImpl(const Parameters &param, File &file);
    ~WalImpl() override;

    [[nodiscard]] auto read(Id page_id, char *page) -> Status override;
    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto needs_checkpoint() const -> bool override;
    [[nodiscard]] auto checkpoint(File &db_file) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto abort() -> Status override;
    [[nodiscard]] auto close() -> Status override;

    [[nodiscard]] auto statistics() const -> WalStatistics override
    {
        return m_stats;
    }

private:
    [[nodiscard]] auto frame_offset(U32 frame) const -> std::size_t;
    [[nodiscard]] auto rewrite_checksums() -> Status;
    [[nodiscard]] auto write_index_header() -> Status;

    WalStatistics m_stats;
    HashIndexHeader m_hdr;
    HashIndex m_index;

    std::string m_scratch;
    std::string m_filename;

    std::size_t m_page_size = 0;
    U32 m_redo_cksum = 0;
    U32 m_backfill = 0;

    Env *m_env = nullptr;
    File *m_file = nullptr;

    U32 m_min_frame = 1;
};

auto Wal::open(const Parameters &param, Wal *&out) -> Status
{
    File *file;
    CALICODB_TRY(param.env->new_file(param.filename, file));
    out = new WalImpl(param, *file);
    return Status::ok();
}

auto Wal::close(Wal *&wal) -> Status
{
    Status s;
    if (wal != nullptr) {
        s = wal->close();
        delete wal;
        wal = nullptr;
    }
    return s;
}

Wal::~Wal() = default;

WalImpl::WalImpl(const Parameters &param, File &file)
    : m_index(m_hdr),
      m_scratch(WalFrameHeader::kSize + param.page_size, '\0'),
      m_filename(param.filename),
      m_page_size(param.page_size),
      m_env(param.env),
      m_file(&file),
      m_min_frame(1)
{
    std::memset(&m_hdr, 0, sizeof(m_hdr));

    (void)m_env;
    (void)m_backfill;
}

WalImpl::~WalImpl()
{
    delete m_file;
}

static auto compute_checksum(const Slice &in, const U32 *initial = nullptr)
{
    CALICODB_EXPECT_GE(in.size(), 8);
    CALICODB_EXPECT_EQ(in.size() & 7, 0);
    CALICODB_EXPECT_LE(in.size(), 65'536);

    //    const auto *data = reinterpret_cast<const U32 *>(in.data());
    //    const auto *end = data + in.size() / sizeof(U32);
    (void)in;

    U32 s1 = 0;
    U32 s2 = 0;
    if (initial != nullptr) {
        s1 = initial[0];
        s2 = initial[1];
    }

    //    do {
    //        s1 += *data++ + s2;
    //        s2 += *data++ + s1;
    //    } while (data < end);

    return std::make_pair(s1, s2);
}

auto WalImpl::rewrite_checksums() -> Status
{
    compute_checksum(Slice(), nullptr);
    return Status::ok();
}

auto WalImpl::write_index_header() -> Status
{
    std::memcpy(m_index.header(), &m_hdr, sizeof(m_hdr));
    // TODO: Memory map the first page if it is not already mapped.
    return Status::ok();
}

auto WalImpl::read(Id page_id, char *page) -> Status
{
    U32 frame;
    CALICODB_TRY(m_index.lookup(page_id.value, m_min_frame, frame));

    if (frame) {
        CALICODB_TRY(m_file->read_exact(
            frame_offset(frame),
            WalFrameHeader::kSize + m_page_size,
            m_scratch.data()));

        WalFrameHeader header;
        header.read(m_scratch.data());

        std::memcpy(page, m_scratch.data() + WalFrameHeader::kSize, m_page_size);
        m_stats.bytes_read += WalFrameHeader::kSize + m_page_size;
        return Status::ok();
    }
    return Status::not_found("wal does not contain page");
}

static auto write_frame(File &file, const WalFrameHeader &header, const Slice &page, std::size_t offset)
{
    char buffer[WalFrameHeader::kSize];
    auto *data = buffer;

    put_u32(data, header.pgno);
    data += sizeof(U32);

    put_u32(data, header.db_size);
    data += sizeof(U32);

    for (const auto *arr : {header.salt, header.cksum}) {
        put_u32(data, arr[0]);
        data += sizeof(U32);

        put_u32(data, arr[1]);
        data += sizeof(U32);
    }

    CALICODB_TRY(file.write(offset, Slice(buffer, WalFrameHeader::kSize)));
    return file.write(offset + WalFrameHeader::kSize, page);
}

auto WalImpl::write(const CacheEntry *dirty, std::size_t db_size) -> Status
{
    if (m_hdr.max_frame == 0) {
        // This is the first frame written to the WAL. Write the WAL header.
        //        char buffer[WalHeader::kSize];
        //        auto *data = buffer;
    }

    const auto *live = m_index.header();
    auto first_frame = m_min_frame;

    // Check if the WAL's copy of the index header differs from what is in the index. If so,
    // then the WAL has been written since the last commit, with the first record located at
    // frame ID "first".
    if (std::memcmp(&m_hdr, live, sizeof(HashIndexHeader)) != 0) {
        first_frame = live->max_frame + 1;
    }

    // Write each dirty page to the WAL.
    auto next_frame = m_hdr.max_frame + 1;
    for (auto *p = dirty; p; p = p->next, ++next_frame) {
        const Slice page(p->page, m_page_size);
        U32 frame;

        // Condition ensures that if this set of pages is a commit, then the last
        // frame will always be appended, even if another copy of it exists in the
        // WAL. This frame needs to have its "db_size" field set to mark that it
        // is a commit frame.
        if (first_frame && (p->next || db_size == 0)) {
            // Check to see if the page has been written to the WAL already by the
            // current transaction. If so, overwrite it and indicate that checksums
            // need to be recomputed from here on commit.
            CALICODB_TRY(m_index.lookup(p->page_id.value, first_frame, frame));
            if (frame) {
                if (m_redo_cksum == 0 || frame < m_redo_cksum) {
                    m_redo_cksum = frame;
                }
                const auto offset = frame_offset(frame) + WalFrameHeader::kSize;
                CALICODB_TRY(m_file->write(offset, page));
                continue;
            }
        }
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it.
        WalFrameHeader header;
        header.pgno = p->page_id.value;
        header.db_size = p->next == nullptr ? static_cast<U32>(db_size) : 0;
        CALICODB_TRY(write_frame(*m_file, header, page, frame_offset(next_frame)));
        m_stats.bytes_written += WalFrameHeader::kSize + m_page_size;

        // TODO: SQLite does this after the loop finishes, in another loop. They pad out remaining frames to make a full sector
        //       if some option is set, to guard against torn writes.
        CALICODB_TRY(m_index.assign(header.pgno, next_frame));
    }

    m_hdr.max_frame = next_frame;
    if (db_size) {
        ++m_hdr.change;
        m_hdr.page_count = static_cast<U32>(db_size);
        return write_index_header();
    }

    return Status::ok();
}

auto WalImpl::needs_checkpoint() const -> bool
{
    return m_hdr.max_frame > 1'000;
}

auto WalImpl::checkpoint(File &db_file) -> Status
{
    CALICODB_TRY(m_file->sync());

    if (m_hdr.max_frame == 0) {
        return Status::ok();
    }
    HashIterator itr(m_index);
    for (;;) {
        HashIterator::Entry entry;
        if (!itr.read(entry)) {
            break;
        }

        CALICODB_TRY(m_file->read_exact(
            frame_offset(entry.value),
            WalFrameHeader::kSize + m_page_size,
            m_scratch.data()));

        CALICODB_TRY(db_file.write(
            (entry.key - 1) * m_page_size,
            Slice(m_scratch.data() + WalFrameHeader::kSize, m_page_size)));

        // TODO: Should increase backfill count here.
    }
    m_min_frame = 1;
    m_hdr.max_frame = 0;
    m_index.cleanup();
    return Status::ok();
}

auto WalImpl::sync() -> Status
{
    return m_file->sync();
}

auto WalImpl::abort() -> Status
{
    if (m_hdr.max_frame) {
        m_hdr = *m_index.header();
    }
    return Status::ok();
}

auto WalImpl::close() -> Status
{
    if (m_hdr.max_frame == 0) {
        return m_env->remove_file(m_filename);
    }
    return Status::ok();
}

auto WalImpl::frame_offset(U32 frame) const -> std::size_t
{
    CALICODB_EXPECT_GT(frame, 0);
    return WalHeader::kSize + (frame - 1) * (WalFrameHeader::kSize + m_page_size);
}

} // namespace calicodb
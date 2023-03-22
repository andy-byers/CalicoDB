// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_RECORD_H
#define CALICODB_WAL_RECORD_H

#include "calicodb/env.h"
#include "delta.h"
#include "encoding.h"
#include "header.h"
#include <algorithm>
#include <map>
#include <variant>

namespace calicodb
{

struct LogicalPageId;

enum WalPayloadType : char {
    kCommitPayload,
    kDeltaPayload,
    kImagePayload,
    kVacuumPayload,
    kNumPayloadTypes,
};

enum WalRecordType : char {
    kNoRecord,
    kFullRecord,
    kFirstRecord,
    kMiddleRecord,
    kLastRecord,
    kNumRecordTypes,
};

// WAL record format (based off RocksDB):
//      Offset  Size  Field
//     ---------------------------
//      0       1     Type
//      1       2     Size
//      3       4     CRC
//
// The CRC field contains the CRC of the type byte, as well as the payload
// fragment that follows the header.
struct WalRecordHeader {
    static constexpr std::size_t kSize {7};

    [[nodiscard]] static auto contains_record(const Slice &data) -> bool
    {
        return data.size() > WalRecordHeader::kSize && data[0] != '\x00';
    }

    WalRecordType type {};
    std::uint16_t size {};
    std::uint32_t crc {};
};

// Routines for working with WAL records.
[[nodiscard]] auto read_wal_record_header(Slice in) -> WalRecordHeader;
[[nodiscard]] auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status;

// Delta payload format:
//
//      Offset  Size  Field
//     ---------------------------
//      0       1     Flags
//      1       8     LSN
//      9       8     Page ID
//      17      2     Delta count
//      19      n     Delta content
//
// Each delta in the delta content area is an (offset, size, data) triplet. "offset" describes
// where on the page the change took place, and "size" is the number of bytes in "data". Both
// "offset" and "size" are 16-bit unsigned integers.
struct DeltaDescriptor {
    static constexpr std::size_t kFixedSize {19};

    struct Delta {
        std::size_t offset {};
        Slice data {};
    };

    Id page_id;
    Lsn lsn;
    std::vector<Delta> deltas;
};

// Image payload header:
//
//      Offset  Size  Field
//     ---------------------------
//      0       1     Flags
//      1       8     LSN
//      9       8     Page ID
//      17      n     Image
//
// The image can be any size less than or equal to the database page size. Its size is not
// stored explicitly in the payload: it is known from the total size of the record fragments
// it is composed from.
struct ImageDescriptor {
    static constexpr std::size_t kFixedSize {17};

    Id page_id;
    Lsn lsn;
    Slice image;
};

// Vacuum records signify the start or end of a vacuum operation.
//
//      Offset  Size  Field
//     ---------------------------
//      0       1     Flags
//      1       8     LSN
//      9       1     Start
struct VacuumDescriptor {
    static constexpr std::size_t kFixedSize {10};

    Lsn lsn;
    bool is_start {};
};

using PayloadDescriptor = std::variant<
    std::monostate,
    DeltaDescriptor,
    ImageDescriptor,
    VacuumDescriptor>;

[[nodiscard]] auto extract_payload_lsn(const Slice &in) -> Lsn;
[[nodiscard]] auto decode_payload(const Slice &in) -> PayloadDescriptor;
[[nodiscard]] auto encode_deltas_payload(Lsn lsn, Id page_id, const Slice &image, const std::vector<PageDelta> &deltas, char *buffer) -> Slice;
[[nodiscard]] auto encode_image_payload(Lsn lsn, Id page_id, const Slice &image, char *buffer) -> Slice;
[[nodiscard]] auto encode_vacuum_payload(Lsn lsn, bool is_start, char *buffer) -> Slice;

static constexpr std::size_t kWalBlockScale {4};

[[nodiscard]] inline constexpr auto wal_block_size(std::size_t page_size) -> std::size_t
{
    return std::min(kMaxPageSize, page_size * kWalBlockScale);
}

[[nodiscard]] inline constexpr auto wal_scratch_size(std::size_t page_size) -> std::size_t
{
    return page_size + DeltaDescriptor::kFixedSize + sizeof(PageDelta);
}

[[nodiscard]] inline auto decode_segment_name(const Slice &prefix, const Slice &path) -> Id
{
    if (path.size() <= prefix.size() || !path.starts_with(prefix)) {
        return Id::null();
    }
    auto name = path.range(prefix.size());

    // Don't call std::stoul() if it's going to fail.
    const auto is_valid = std::all_of(name.data(), name.data() + name.size(), [](auto c) {
        return std::isdigit(c);
    });

    if (!is_valid) {
        return Id::null();
    }

    return {std::stoull(name.to_string())};
}

[[nodiscard]] inline auto encode_segment_name(const Slice &prefix, Id id) -> std::string
{
    return prefix.to_string() + std::to_string(id.value);
}

template <class Itr>
[[nodiscard]] inline auto cache_first_lsn(Env &env, const std::string &prefix, Itr &itr) -> Status
{
    if (auto lsn = itr->second; !lsn.is_null()) {
        return Status::ok();
    }

    Reader *temp;
    CALICODB_TRY(env.new_reader(encode_segment_name(prefix, itr->first), temp));
    std::unique_ptr<Reader> file {temp};

    Slice slice;
    char buffer[sizeof(Lsn)];
    CALICODB_TRY(file->read(WalRecordHeader::kSize + 1, sizeof(buffer), buffer, &slice));

    itr->second = Lsn::null();
    if (slice.size() != sizeof(buffer)) {
        return Status::not_found("unable to read first LSN");
    }
    itr->second.value = get_u64(slice);
    return Status::ok();
}

} // namespace calicodb

#endif // CALICODB_WAL_RECORD_H
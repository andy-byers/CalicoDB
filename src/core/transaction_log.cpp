#include "transaction_log.h"

namespace calico {

auto encode_deltas_payload(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::DELTAS);
    out.advance();

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Deltas count (2 B)
    put_u16(out, static_cast<std::uint16_t>(deltas.size()));
    out.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (const auto &[offset, size]: deltas) {
        put_u16(out, static_cast<std::uint16_t>(offset));
        out.advance(sizeof(std::uint16_t));

        put_u16(out, static_cast<std::uint16_t>(size));
        out.advance(sizeof(std::uint16_t));

        mem_copy(out, image.range(offset, size));
        out.advance(size);
    }
    return original_size - out.size();
}

auto encode_commit_payload(Bytes out) -> Size
{
    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::COMMIT);
    out.advance();

    return MINIMUM_PAYLOAD_SIZE;
}

auto encode_full_image_payload(PageId page_id, BytesView image, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::FULL_IMAGE);
    out.advance();

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Image (N B)
    mem_copy(out, image);
    out.advance(image.size());

    return original_size - out.size();
}

static auto decode_deltas_payload(BytesView in) -> DeltasDescriptor
{
    DeltasDescriptor info;

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {in[0]}, XactPayloadType::DELTAS);
    in.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(in);
    in.advance(sizeof(info.pid));

    // Deltas count (2 B)
    info.deltas.resize(get_u16(in));
    in.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (auto &[offset, bytes]: info.deltas) {
        offset = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        const auto size = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        bytes = in.range(0, size);
        in.advance(size);
    }
    return info;
}

static auto decode_full_image_payload(BytesView in) -> FullImageDescriptor
{
    CALICO_EXPECT_EQ(XactPayloadType {in[0]}, XactPayloadType::FULL_IMAGE);
    in.advance();

    FullImageDescriptor info {};
    info.pid.value = get_u64(in);
    in.advance(sizeof(PageId));
    info.image = in;
    return info;
}

static auto decode_commit_payload(BytesView in) -> CommitDescriptor
{
    CALICO_EXPECT_EQ(XactPayloadType {in[0]}, XactPayloadType::COMMIT);
    return CommitDescriptor {};
}

auto decode_payload(BytesView in) -> std::optional<PayloadDescriptor>
{
    switch (XactPayloadType {in[0]}) {
        case XactPayloadType::DELTAS:
            return decode_deltas_payload(in);
        case XactPayloadType::FULL_IMAGE:
            return decode_full_image_payload(in);
        case XactPayloadType::COMMIT:
            return decode_commit_payload(in);
        default:
            return std::nullopt;
    }
}

} // namespace calico
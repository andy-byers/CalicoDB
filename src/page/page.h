#ifndef CALICO_PAGE_PAGE_H
#define CALICO_PAGE_PAGE_H

#include "utils/types.h"
#include "core/transaction_log.h"
#include <optional>
#include <vector>

namespace calico {

struct FileHeader;
class Frame;
class Pager;

class Page final {
public:
    friend class calico::Frame;

    struct Parameters {
        PageId id;
        Bytes data;
        Pager *source {};
        bool is_writable {};
        bool is_dirty {};
    };

    ~Page();
    explicit Page(const Parameters &);

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_is_writable;
    }

    [[nodiscard]] auto is_dirty() const -> bool
    {
        return m_is_dirty;
    }

    [[nodiscard]] auto id() const -> PageId;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto view(Size) const -> BytesView;
    [[nodiscard]] auto view(Size, Size) const -> BytesView;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> SequenceId;
    [[nodiscard]] auto collect_deltas() -> std::vector<PageDelta>;
    auto set_type(PageType) -> void;
    auto set_lsn(SequenceId) -> void;
    auto read(Bytes, Size) const -> void;
    auto bytes(Size) -> Bytes;
    auto bytes(Size, Size) -> Bytes;
    auto write(BytesView, Size) -> void;
    auto apply_update(const DeltasDescriptor &) -> void;
    auto apply_update(const FullImageDescriptor&) -> void;

    // NOTE: We need these because we have a user-defined destructor.
    Page(Page &&) = default;
    auto operator=(Page &&) -> Page & = default;

private:
    [[nodiscard]] auto header_offset() const -> Size;

    std::vector<PageDelta> m_deltas;
    UniqueNullable<Pager *> m_source;
    Bytes m_data;
    PageId m_id;
    bool m_is_writable {};
    bool m_is_dirty {};
};

[[nodiscard]] auto get_u16(const Page &, Size) -> std::uint16_t;
[[nodiscard]] auto get_u32(const Page &, Size) -> std::uint32_t;
[[nodiscard]] auto get_u64(const Page &, Size) -> std::uint64_t;
auto put_u16(Page &, Size, std::uint16_t) -> void;
auto put_u32(Page &, Size, std::uint32_t) -> void;
auto put_u64(Page &, Size, std::uint64_t) -> void;

} // namespace calico

#endif // CALICO_PAGE_PAGE_H

#ifndef CALICO_PAGE_PAGE_H
#define CALICO_PAGE_PAGE_H

#include "core/recovery.h"
#include "utils/types.h"
#include <optional>
#include <vector>

namespace Calico {

struct FileHeader;
class Frame;
class Pager;

class Page final {
public:
    friend class Calico::Frame;

    struct Parameters {
        Id id;
        Bytes data;
        Pager *source {};
        bool is_writable {};
    };

    ~Page();
    explicit Page(const Parameters &);

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_is_writable;
    }

    [[nodiscard]] auto id() const -> Id;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto view(Size) const -> BytesView;
    [[nodiscard]] auto view(Size, Size) const -> BytesView;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> Id;
    [[nodiscard]] auto collect_deltas() -> std::vector<PageDelta>;
    auto set_type(PageType) -> void;
    auto set_lsn(Id) -> void;
    auto read(Bytes, Size) const -> void;
    auto bytes(Size) -> Bytes;
    auto bytes(Size, Size) -> Bytes;
    auto write(BytesView, Size) -> void;
    auto apply_update(const DeltaDescriptor &) -> void;
    auto apply_update(const FullImageDescriptor&) -> void;

    // NOTE: We need these because we have a user-defined destructor.
    Page(Page &&) = default;
    auto operator=(Page &&) -> Page & = default;

private:
    [[nodiscard]] auto header_offset() const -> Size;

    std::vector<PageDelta> m_deltas;
    UniqueNullable<Pager *> m_source;
    Bytes m_data;
    Id m_id;
    bool m_is_writable {};
};

[[nodiscard]] auto get_u16(const Page &, Size) -> std::uint16_t;
[[nodiscard]] auto get_u32(const Page &, Size) -> std::uint32_t;
[[nodiscard]] auto get_u64(const Page &, Size) -> std::uint64_t;
auto put_u16(Page &, Size, std::uint16_t) -> void;
auto put_u32(Page &, Size, std::uint32_t) -> void;
auto put_u64(Page &, Size, std::uint64_t) -> void;

} // namespace Calico

#endif // CALICO_PAGE_PAGE_H

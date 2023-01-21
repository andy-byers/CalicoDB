#ifndef CALICO_PAGE_PAGE_H
#define CALICO_PAGE_PAGE_H

#include <optional>
#include <vector>
#include "delta.h"
#include "utils/types.h"
#include "wal/record.h"

namespace Calico {

struct FileHeader;
class Frame;
class Pager;

class Page final {
public:
    friend class Calico::Frame;

    struct Parameters {
        Id id;
        Span data;
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
    [[nodiscard]] auto view(Size) const -> Slice;
    [[nodiscard]] auto view(Size, Size) const -> Slice;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> Id;
    [[nodiscard]] auto collect_deltas() -> std::vector<PageDelta>;
    auto set_type(PageType) -> void;
    auto set_lsn(Id) -> void;
    auto read(Span, Size) const -> void;
    auto span(Size) -> Span;
    auto span(Size, Size) -> Span;
    auto write(Slice, Size) -> void;
    auto apply_update(const DeltaDescriptor &) -> void;
    auto apply_update(const FullImageDescriptor&) -> void;

    // NOTE: We need these because we have a user-defined destructor.
    Page(Page &&) = default;
    auto operator=(Page &&) -> Page & = default;

private:
    [[nodiscard]] auto header_offset() const -> Size;

    std::vector<PageDelta> m_deltas;
    UniqueNullable<Pager *> m_source;
    Span m_data;
    Id m_id;
    bool m_is_writable {};
};

[[nodiscard]] auto get_u16(const Page &page, Size offset) -> std::uint16_t;
[[nodiscard]] auto get_u32(const Page &page, Size offset) -> std::uint32_t;
[[nodiscard]] auto get_u64(const Page &page, Size offset) -> std::uint64_t;
auto put_u16(Page &page, Size offset, std::uint16_t value) -> void;
auto put_u32(Page &page, Size offset, std::uint32_t value) -> void;
auto put_u64(Page &page, Size offset, std::uint64_t value) -> void;

} // namespace Calico

#endif // CALICO_PAGE_PAGE_H

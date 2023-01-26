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

class Page_ final {
public:
    friend class Calico::Frame;

    struct Parameters {
        Id id;
        Span data;
        Pager *source {};
        bool is_writable {};
    };

    ~Page_();
    explicit Page_(const Parameters &);

    [[nodiscard]] auto is_writable() const -> bool
    {
        return m_is_writable;
    }

    [[nodiscard]] auto id() const -> Id;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto view(Size) const -> Slice;
    [[nodiscard]] auto view(Size, Size) const -> Slice;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> Lsn;
    [[nodiscard]] auto collect_deltas() -> std::vector<PageDelta>;
    auto set_type(PageType) -> void;
    auto set_lsn(Lsn) -> void;
    auto read(Span, Size) const -> void;
    auto span(Size) -> Span;
    auto span(Size, Size) -> Span;
    auto write(const Slice &, Size) -> void;
    auto apply_update(const DeltaDescriptor &) -> void;
    auto apply_update(const FullImageDescriptor&) -> void;

    // NOTE: We need these because we have a user-defined destructor.
    Page_(Page_ &&) = default;
    auto operator=(Page_ &&) -> Page_ & = default;

private:
    std::vector<PageDelta> m_deltas;
    UniqueNullable<Pager *> m_source;
    Span m_data;
    Id m_id;
    Size m_header_offset {};
    bool m_is_writable {};
};

[[nodiscard]] auto get_u16(const Page_ &page, Size offset) -> std::uint16_t;
[[nodiscard]] auto get_u32(const Page_ &page, Size offset) -> std::uint32_t;
[[nodiscard]] auto get_u64(const Page_ &page, Size offset) -> std::uint64_t;
auto put_u16(Page_ &page, Size offset, std::uint16_t value) -> void;
auto put_u32(Page_ &page, Size offset, std::uint32_t value) -> void;
auto put_u64(Page_ &page, Size offset, std::uint64_t value) -> void;

} // namespace Calico

#endif // CALICO_PAGE_PAGE_H

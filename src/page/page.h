#ifndef CCO_PAGE_PAGE_H
#define CCO_PAGE_PAGE_H

#include "calico/wal.h"
#include "utils/identifier.h"
#include "utils/types.h"
#include <optional>
#include <vector>

namespace cco {

struct ChangedRegion;
struct PageUpdate;
class Frame;
class FileHeaderReader;
class FileHeaderWriter;
class Pager;

class Page final {
public:
    friend class cco::Frame;

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
    [[nodiscard]] auto view(Index) const -> BytesView;
    [[nodiscard]] auto view(Index, Size) const -> BytesView;
    [[nodiscard]] auto type() const -> PageType;
    [[nodiscard]] auto lsn() const -> SequenceNumber;
    [[nodiscard]] auto deltas() const -> std::vector<PageDelta>;
    auto set_type(PageType) -> void;
    auto set_lsn(SequenceNumber) -> void;
    auto read(Bytes, Index) const -> void;
    auto bytes(Index) -> Bytes;
    auto bytes(Index, Size) -> Bytes;
    auto write(BytesView, Index) -> void;
    auto undo(const UndoDescriptor&) -> void;
    auto redo(const RedoDescriptor&) -> void;

    // NOTE: We need these because we have a user-defined destructor.
    Page(Page &&) = default;
    auto operator=(Page &&) -> Page & = default;

private:
    [[nodiscard]] auto header_offset() const -> Index;

    std::vector<PageDelta> m_deltas;
    UniqueNullable<Pager *> m_source;
    Bytes m_data;
    PageId m_id;
    bool m_is_writable {};
    bool m_is_dirty {};
};

[[nodiscard]] auto get_u16(const Page &, Index) -> std::uint16_t;
[[nodiscard]] auto get_u32(const Page &, Index) -> std::uint32_t;
[[nodiscard]] auto get_u64(const Page &, Index) -> std::uint64_t;
auto put_u16(Page &, Index, std::uint16_t) -> void;
auto put_u32(Page &, Index, std::uint32_t) -> void;
auto put_u64(Page &, Index, std::uint64_t) -> void;

[[nodiscard]] auto get_file_header_reader(const Page &) -> FileHeaderReader;
[[nodiscard]] auto get_file_header_writer(Page &) -> FileHeaderWriter;

} // namespace cco

#endif // CCO_PAGE_PAGE_H

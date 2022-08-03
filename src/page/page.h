#ifndef CCO_PAGE_PAGE_H
#define CCO_PAGE_PAGE_H

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
class IBufferPool;
class ChangeManager;

class Page final {
public:
    friend class cco::Frame;

    struct Parameters {
        PageId id;
        Bytes data;
        IBufferPool *source {};
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
    auto set_type(PageType) -> void;
    auto set_lsn(SequenceNumber) -> void;
    auto read(Bytes, Index) const -> void;
    auto bytes(Index) -> Bytes;
    auto bytes(Index, Size) -> Bytes;
    auto write(BytesView, Index) -> void;
    auto undo(SequenceNumber, const std::vector<ChangedRegion> &) -> void;
    auto redo(SequenceNumber, const std::vector<ChangedRegion> &) -> void;

    [[nodiscard]] auto has_manager() const -> bool
    {
        return m_manager != nullptr;
    }

    auto set_manager(ChangeManager &manager) -> void
    {
        CCO_EXPECT_EQ(m_manager, nullptr);
        m_manager = &manager;
    }

    auto clear_manager() -> void
    {
        CCO_EXPECT_NE(m_manager, nullptr);
        m_manager = nullptr;
    }

    Page(Page &&) noexcept = default;
    auto operator=(Page &&) noexcept -> Page & = default;
    Page(const Page &) noexcept = delete;
    auto operator=(const Page &) noexcept -> Page & = delete;

private:
    [[nodiscard]] auto header_offset() const -> Index;

    UniqueNullable<IBufferPool *> m_source;
    ChangeManager *m_manager {};
    Bytes m_data;
    PageId m_id;
    bool m_is_writable {};
    bool m_is_dirty {};
};

[[nodiscard]] auto get_u16(const Page &, Index) -> uint16_t;
[[nodiscard]] auto get_u32(const Page &, Index) -> uint32_t;
auto put_u16(Page &, Index, uint16_t) -> void;
auto put_u32(Page &, Index, uint32_t) -> void;

[[nodiscard]] auto get_file_header_reader(const Page &) -> FileHeaderReader;
[[nodiscard]] auto get_file_header_writer(Page &) -> FileHeaderWriter;

} // namespace cco

#endif // CCO_PAGE_PAGE_H

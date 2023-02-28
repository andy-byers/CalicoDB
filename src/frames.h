#ifndef CALICODB_FRAMES_H
#define CALICODB_FRAMES_H

#include "calicodb/status.h"
#include "types.h"
#include <list>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace calicodb
{

struct FileHeader;
class Page;
class Pager;
class Editor;
class Env;

class Frame final
{
public:
    Frame(char *buffer, std::size_t id, std::size_t size);

    [[nodiscard]] auto pid() const -> Id
    {
        return m_page_id;
    }

    [[nodiscard]] auto ref_count() const -> std::size_t
    {
        return m_ref_count;
    }

    [[nodiscard]] auto data() const -> Slice
    {
        return m_bytes;
    }

    [[nodiscard]] auto data() -> Span
    {
        return m_bytes;
    }

    auto reset(Id id) -> void
    {
        CDB_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
    }

    [[nodiscard]] auto lsn() const -> Id;
    auto ref(Page &pagez) -> void;
    auto upgrade(Page &page) -> void;
    auto unref(Page &page) -> void;

private:
    Span m_bytes;
    Id m_page_id;
    std::size_t m_ref_count {};
    bool m_is_writable {};
};

class FrameManager final
{
public:
    friend class DBImpl;
    friend class Pager;

    explicit FrameManager(Editor &file, AlignedBuffer buffer, std::size_t page_size, std::size_t frame_count);
    ~FrameManager() = default;
    [[nodiscard]] auto write_back(std::size_t index) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto pin(Id pid, std::size_t &fid) -> Status;
    auto unpin(std::size_t) -> void;
    auto ref(std::size_t index, Page &out) -> void;
    auto unref(std::size_t index, Page page) -> void;
    auto upgrade(std::size_t index, Page &page) -> void;
    auto load_state(const FileHeader &header) -> void;
    auto save_state(FileHeader &header) const -> void;

    [[nodiscard]] auto page_count() const -> std::size_t
    {
        return m_page_count;
    }

    [[nodiscard]] auto page_size() const -> std::size_t
    {
        return m_page_size;
    }

    [[nodiscard]] auto available() const -> std::size_t
    {
        return m_available.size();
    }

    [[nodiscard]] auto ref_sum() const -> std::size_t
    {
        return m_ref_sum;
    }

    [[nodiscard]] auto get_frame(std::size_t index) const -> const Frame &
    {
        CDB_EXPECT_LT(index, m_frames.size());
        return m_frames[index];
    }

    [[nodiscard]] auto bytes_written() const -> std::size_t
    {
        return m_bytes_written;
    }

    auto operator=(FrameManager &&) -> FrameManager & = default;
    FrameManager(FrameManager &&) = default;

private:
    [[nodiscard]] auto read_page_from_file(Id, Span) const -> Status;
    [[nodiscard]] auto write_page_to_file(Id pid, const Slice &page) const -> Status;

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<std::size_t> m_available;
    std::unique_ptr<Editor> m_file;
    std::size_t m_page_count {};
    std::size_t m_page_size {};
    std::size_t m_ref_sum {};
    std::size_t m_bytes_written {};
};

} // namespace calicodb

#endif // CALICODB_FRAMES_H

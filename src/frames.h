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

struct Frame
{
    explicit Frame(char *buffer);

    [[nodiscard]] auto lsn() const -> Id;
    auto ref() -> void;
    auto upgrade() -> void;
    auto unref() -> void;

    char *data;
    Id page_id;
    std::size_t ref_count {};
    bool write {};
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
    [[nodiscard]] auto pin(Id page_id, std::size_t &index) -> Status;
    auto unpin(std::size_t index) -> void;
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
    [[nodiscard]] auto read_page_from_file(Id page_id, char *out) const -> Status;
    [[nodiscard]] auto write_page_to_file(Id page_id, const char *in) const -> Status;

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

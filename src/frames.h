// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FRAMES_H
#define CALICODB_FRAMES_H

#include "utils.h"
#include <list>
#include <vector>

namespace calicodb
{

struct FileHeader;
class Page;
class Pager;
class Editor;
class Env;

struct Frame {
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

class AlignedBuffer
{
public:
    AlignedBuffer(std::size_t size, std::size_t alignment)
        : m_data {
              new (std::align_val_t {alignment}, std::nothrow) char[size](),
              Deleter {std::align_val_t {alignment}},
          }
    {
        CALICODB_EXPECT_TRUE(is_power_of_two(alignment));
        CALICODB_EXPECT_EQ(size % alignment, 0);
    }

    [[nodiscard]] auto get() -> char *
    {
        return m_data.get();
    }

    [[nodiscard]] auto get() const -> const char *
    {
        return m_data.get();
    }

private:
    struct Deleter {
        auto operator()(char *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<char[], Deleter> m_data;
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
    auto unref(std::size_t index, Page) -> void;
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
        CALICODB_EXPECT_LT(index, m_frames.size());
        return m_frames[index];
    }

    [[nodiscard]] auto bytes_read() const -> std::size_t
    {
        return m_bytes_read;
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

    mutable std::size_t m_bytes_read {};
    mutable std::size_t m_bytes_written {};
};

} // namespace calicodb

#endif // CALICODB_FRAMES_H

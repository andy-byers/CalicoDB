#include "pager.h"

#include "frame.h"
#include "storage/file.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

using namespace utils;

auto Pager::open(Parameters param) -> Result<std::unique_ptr<Pager>>
{
    static constexpr auto ERROR_PRIMARY = "cannot create pager object";
    static constexpr auto ERROR_DETAIL = "frame count is too {}";
    static constexpr auto ERROR_HINT = "{} frame count is {}";
    auto logger = create_logger(param.log_sink, "Pager");

    if (param.frame_count < MINIMUM_FRAME_COUNT) {
        ErrorMessage group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail(ERROR_DETAIL, "small");
        group.set_hint(ERROR_HINT, "minimum", MINIMUM_FRAME_COUNT);
        return Err {group.invalid_argument(*logger)};
    }

    if (param.frame_count > MAXIMUM_FRAME_COUNT) {
        ErrorMessage group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail(ERROR_DETAIL, "large");
        group.set_hint(ERROR_HINT, "maximum", MAXIMUM_FRAME_COUNT);
        return Err {group.invalid_argument(*logger)};
    }

    const auto cache_size = param.page_size * param.frame_count;
    auto buffer = std::unique_ptr<Byte[], AlignedDeleter> {
        new(static_cast<std::align_val_t>(param.page_size), std::nothrow) Byte[cache_size],
        AlignedDeleter {static_cast<std::align_val_t>(param.page_size)}
    };

    if (!buffer) {
        ErrorMessage group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("allocation of {} B cache failed", cache_size);
        group.set_hint("out of memory");
        return Err {group.system_error(*logger)};
    }
    return std::unique_ptr<Pager> {new Pager{std::move(buffer), std::move(param)}};
}

Pager::Pager(AlignedBuffer buffer, Parameters param):
      m_buffer {std::move(buffer)},
      m_reader {std::move(param.reader)},
      m_writer {std::move(param.writer)},
      m_logger {create_logger(param.log_sink, "Pager")},
      m_frame_count {param.frame_count},
      m_page_size {param.page_size}
{
    m_logger->trace("constructing Pager object");

    // The buffer should be aligned to the page size.
    CCO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % m_page_size, 0);
    mem_clear({m_buffer.get(), m_page_size * m_frame_count});

    while (m_available.size() < m_frame_count)
        m_available.emplace_back(m_buffer.get(), m_available.size(), m_page_size);
}

auto Pager::available() const -> Size 
{
    return m_available.size();
}

auto Pager::page_size() const -> Size
{
    return m_page_size;
}

auto Pager::truncate(Size page_count) -> Result<void>
{
    CCO_EXPECT_EQ(m_available.size(), m_frame_count);
    return m_writer->resize(page_count * page_size());
}

auto Pager::pin(PID id) -> Result<Frame>
{
    CCO_EXPECT_FALSE(id.is_null());
    if (m_available.empty())
        m_available.emplace_back(Frame {page_size()});

    auto &frame = m_available.back();

    if (auto result = read_page_from_file(id, frame.data()); !result) {
        return Err {result.error()};
    } else if (!result.value()) {
        mem_clear(frame.data());
    }

    auto moved = std::move(m_available.back());
    m_available.pop_back();
    moved.reset(id);
    return moved;
}

auto Pager::discard(Frame frame) -> Result<void>
{
    frame.reset(PID::null());
    return unpin(std::move(frame));
}

auto Pager::unpin(Frame frame) -> Result<void>
{
    CCO_EXPECT_EQ(frame.ref_count(), 0);
    Result<void> result;

    // If this fails, the caller (buffer pool) will need to roll back the database state or exit.
    if (frame.is_dirty())
        result = write_page_to_file(frame.page_id(), frame.data());

    frame.reset(PID::null());
    if (!frame.is_owned())
        m_available.emplace_back(std::move(frame));
    return result;
}

auto Pager::sync() -> Result<void>
{
    return m_writer->sync();
}

auto Pager::read_page_from_file(PID id, Bytes out) const -> Result<bool>
{
    CCO_EXPECT_EQ(page_size(), out.size());

    return m_reader->read(out, FileLayout::page_offset(id, out.size()))
        .and_then([&](Size read_size) -> Result<bool> {
            if (read_size == 0) {
                // Just hit EOF.
                return false;
            } else if (read_size == out.size()) {
                // Read a full page.
                return true;
            }
            // Partial read.
            ErrorMessage message;
            message.set_primary("cannot read page");
            message.set_detail("read an incomplete page");
            return Err {message.system_error(*m_logger)};
        })
        .or_else([this](const Error &error) -> Result<bool> {
            NumberedGroup group;
            group.push_line("cannot read page");
            group.push_line("[FileReader] {}", btos(error.what()));
            group.log(*m_logger, spdlog::level::err);
            return Err {error};
        });
}

auto Pager::write_page_to_file(PID id, BytesView in) const -> Result<void>
{
    CCO_EXPECT_EQ(page_size(), in.size());

    return write_all(*m_writer, in, FileLayout::page_offset(id, in.size()))
        .or_else([this](const Error &error) -> Result<void> {
            NumberedGroup group;
            group.push_line("cannot write page");
            group.push_line("[FileWriter] {}", btos(error.what()));
            group.log(*m_logger, spdlog::level::err);
            return Err {error};
        });
}

} // calico

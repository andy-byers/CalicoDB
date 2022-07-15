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
        LogMessage message {*logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail(ERROR_DETAIL, "small");
        message.set_hint(ERROR_HINT, "minimum", MINIMUM_FRAME_COUNT);
        return Err {message.invalid_argument()};
    }

    if (param.frame_count > MAXIMUM_FRAME_COUNT) {
        LogMessage message {*logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail(ERROR_DETAIL, "large");
        message.set_hint(ERROR_HINT, "maximum", MAXIMUM_FRAME_COUNT);
        return Err {message.invalid_argument()};
    }

    const auto cache_size = param.page_size * param.frame_count;
    auto buffer = std::unique_ptr<Byte[], AlignedDeleter> {
        new(static_cast<std::align_val_t>(param.page_size), std::nothrow) Byte[cache_size],
        AlignedDeleter {static_cast<std::align_val_t>(param.page_size)}
    };

    if (!buffer) {
        LogMessage message {*logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail("allocation of {} B cache failed", cache_size);
        message.set_hint("out of memory");
        return Err {message.system_error()};
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
    static constexpr auto ERROR_PRIMARY = "cannot read page";
    static constexpr auto ERROR_DETAIL = "page ID is {}";
    CCO_EXPECT_EQ(page_size(), out.size());

    const auto was_read = m_reader->read(out, FileLayout::page_offset(id, out.size()));

    // System call error.
    if (!was_read.has_value()) {
        m_logger->error(btos(was_read.error().what()));
        LogMessage message {*m_logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail(ERROR_DETAIL, id.value);
        message.log();
        return Err {was_read.error()};
    }

    // Just read the whole page.
    if (const auto read_size = *was_read; read_size == out.size()) {
        return true;
    // Just hit EOF.
    } else if (read_size == 0) {
        return false;
    // Incomplete read.
    } else {
        LogMessage message {*m_logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail(ERROR_DETAIL, id.value);
        message.set_hint("incomplete read of {} B", read_size);
        return Err {message.system_error()};
    }
}

auto Pager::write_page_to_file(PID id, BytesView in) const -> Result<void>
{
    CCO_EXPECT_EQ(page_size(), in.size());

    return write_all(*m_writer, in, FileLayout::page_offset(id, in.size()))
        .or_else([this](const Error &error) -> Result<void> {
            m_logger->error(btos(error.what()));
            m_logger->error("cannot write page");
            return Err {error};
        });
}

} // calico

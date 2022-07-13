#include "pager.h"

#include "frame.h"
#include "storage/file.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace calico {

using namespace utils;

Pager::Pager(Parameters param):
      m_buffer {
          std::unique_ptr<Byte[], AlignedDeleter> {
           new(static_cast<std::align_val_t>(param.page_size)) Byte[param.page_size * param.frame_count],
           AlignedDeleter {static_cast<std::align_val_t>(param.page_size)}}
      },
      m_reader {std::move(param.reader)},
      m_writer {std::move(param.writer)},
      m_logger {utils::create_logger(param.log_sink, "Pager")},
      m_frame_count {param.frame_count},
      m_page_size {param.page_size}
{
    static constexpr auto ERROR_PRIMARY = "cannot create pager object";
    static constexpr auto ERROR_DETAIL = "frame count is too {}";
    static constexpr auto ERROR_HINT = "{} frame count is {}";
    m_logger->trace("constructing Pager object");

    // The buffer should be aligned to the page size.
    CALICO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % m_page_size, 0);
    mem_clear({m_buffer.get(), m_page_size * m_frame_count});

    if (m_frame_count < MINIMUM_FRAME_COUNT) {
        utils::ErrorMessage group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail(ERROR_DETAIL, "small");
        group.set_hint(ERROR_HINT, "minimum", MINIMUM_FRAME_COUNT);
        throw std::invalid_argument {group.error(*m_logger)};
    }

    if (m_frame_count > MAXIMUM_FRAME_COUNT) {
        utils::ErrorMessage group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail(ERROR_DETAIL, "large");
        group.set_hint(ERROR_HINT, "maximum", MAXIMUM_FRAME_COUNT);
        throw std::invalid_argument {group.error(*m_logger)};
    }

    while (m_available.size() < m_frame_count)
        m_available.emplace_back(m_buffer.get(), m_available.size(), m_page_size);
}

Pager::~Pager()
{
    // TODO: Should probably be done elsewhere, e.g. an `auto close() -> Result<void>` or something.
    maybe_write_pending()
        .or_else([this](const Error &error) {
            m_logger->error("cannot clean up pager: {} frames were lost", m_pending.size());
            m_logger->error(btos(error.what()));
        });
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
    CALICO_EXPECT_EQ(m_available.size(), m_frame_count);
    return m_writer->resize(page_count * page_size());
}

auto Pager::pin(PID id) -> Result<Frame>
{
    CALICO_EXPECT_FALSE(id.is_null());
    if (auto wrote_pending = maybe_write_pending(); !wrote_pending)
        return ErrorResult {wrote_pending.error()};

    if (m_available.empty())
        m_available.emplace_back(Frame {page_size()});

    auto &frame = m_available.back();

    if (auto result = read_page_from_file(id, frame.data()); !result) {
        return ErrorResult {result.error()};
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
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    if (auto result = maybe_write_pending(); !result) {
        m_pending.emplace_back(std::move(frame));
        return result;
    }

    if (frame.is_dirty()) {
        auto was_written = write_page_to_file(frame.page_id(), frame.data());
        if (!was_written) {
            m_pending.emplace_back(std::move(frame));
            return was_written;
        }
    }

    frame.reset(PID::null());
    if (!frame.is_owned())
        m_available.emplace_back(std::move(frame));
    return {};
}

auto Pager::sync() -> Result<void>
{
    return m_writer->sync();
}

auto Pager::maybe_write_pending() -> Result<void>
{
    for (auto &frame: m_pending) {
        if (!frame.is_dirty())
            continue;
        if (auto result = write_page_to_file(frame.page_id(), frame.data())) {
            frame.reset(PID::null());
        } else {
            return result;
        }
    }
    // This is a NOP if there are no pending frames.
    m_available.splice(end(m_available), m_pending);
    return {};
}

auto Pager::read_page_from_file(PID id, Bytes out) const -> Result<bool>
{
    CALICO_EXPECT_EQ(page_size(), out.size());

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
            utils::ErrorMessage message;
            message.set_primary("cannot read page");
            message.set_detail("read an incomplete page");
            return ErrorResult {message.system_error(*m_logger)};
        })
        .or_else([this](const Error &error) -> Result<bool> {
            utils::NumberedGroup group;
            group.push_line("cannot read page");
            group.push_line("[FileReader] {}", btos(error.what()));
            group.log(*m_logger, spdlog::level::err);
            return ErrorResult {error};
        });
}

auto Pager::write_page_to_file(PID id, BytesView in) const -> Result<void>
{
    CALICO_EXPECT_EQ(page_size(), in.size());

    return write_all(*m_writer, in, FileLayout::page_offset(id, in.size()))
        .or_else([this](const Error &error) -> Result<void> {
            utils::NumberedGroup group;
            group.push_line("cannot write page");
            group.push_line("[FileWriter] {}", btos(error.what()));
            group.log(*m_logger, spdlog::level::err);
            return ErrorResult {error};
        });
}

} // calico

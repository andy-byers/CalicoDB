#include "node_pool.h"
#include "page/cell.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/node.h"
#include "page/page.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

using namespace page;
using namespace utils;

NodePool::NodePool(Parameters param)
    : m_free_list {{param.buffer_pool, param.free_start, param.free_count}},
      m_pool {param.buffer_pool},
      m_node_count {param.node_count}
{}

auto NodePool::page_size() const -> Size
{
    return m_pool->page_size();
}

auto NodePool::allocate(PageType type) -> Result<Node>
{
    auto page = m_free_list.pop()
        .or_else([this](const Status &error) -> Result<Page> {
            if (error.is_logic_error())
                return m_pool->allocate();
            return Err {error};
        });
    if (page) {
        page->set_type(type);
        m_node_count++;
        return Node {std::move(*page), true};
    }
    return Err {page.error()};
}

auto NodePool::acquire(PID id, bool is_writable) -> Result<Node>
{
    return m_pool->acquire(id, is_writable)
        .and_then([](Page page) -> Result<Node> {
            return Node {std::move(page), false};
        });
}

auto NodePool::release(Node node) -> Result<void>
{
    CCO_EXPECT_FALSE(node.is_overflowing());
    return m_pool->release(node.take());
}

auto NodePool::destroy(Node node) -> Result<void>
{
    CCO_EXPECT_FALSE(node.is_overflowing());
    return m_free_list.push(node.take())
        .map([this] {m_node_count--;});
}

auto NodePool::allocate_chain(BytesView overflow) -> Result<PID>
{
    CCO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = PID::null();

    while (!overflow.is_empty()) {
        auto page = m_free_list.pop()
            .or_else([this](const Status &error) -> Result<Page> {
                if (error.is_logic_error())
                    return m_pool->allocate();
                return Err {error};
            });
        if (!page.has_value())
            return Err {page.error()};

        page->set_type(PageType::OVERFLOW_LINK);
        Link link {std::move(*page)};
        auto content = link.content_bytes(std::min(overflow.size(), link.content_size()));
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            prev->set_next_id(link.id());
            CCO_TRY(m_pool->release(prev->take()));
        } else {
            head = link.id();
        }
        prev.emplace(std::move(link));
    }
    if (prev)
        CCO_TRY(m_pool->release(prev->take()));
    return head;
}

auto NodePool::collect_chain(PID id, Bytes out) const -> Result<void>
{
    while (!out.is_empty()) {
        CCO_TRY_CREATE(page, m_pool->acquire(id, false));
        if (page.type() != PageType::OVERFLOW_LINK) {
//            utils::ErrorMessage message;
//            message.set_primary("cannot collect overflow chain");
//            message.set_detail("link has an invalid page type {}", static_cast<unsigned>(page->type()));
            return Err {Status::corruption("")};
        }
        Link link {std::move(page)};
        auto content = link.content_view();
        const auto chunk = std::min(out.size(), content.size());
        mem_copy(out, content, chunk);
        out.advance(chunk);
        id = link.next_id();
        CCO_TRY(m_pool->release(link.take()));
    }
    return {};
}

auto NodePool::destroy_chain(PID id, Size size) -> Result<void>
{
    while (size) {
        auto page = m_pool->acquire(id, true);
        if (!page.has_value())
            return Err {page.error()};
        CCO_EXPECT_EQ(page->type(), PageType::OVERFLOW_LINK); // TODO: Corruption error, not assertion. Need a logger for this class.
        Link link {std::move(*page)};
        id = link.next_id();
        size -= std::min(size, link.content_view().size());
        CCO_TRY(m_free_list.push(link.take()));
    }
    return {};
}

auto NodePool::save_header(FileHeaderWriter &header) -> void
{
    header.set_node_count(m_node_count);
    m_free_list.save_header(header);
}

auto NodePool::load_header(const FileHeaderReader &header) -> void
{
    m_node_count = header.node_count();
    m_free_list.load_header(header);
}

} // cco

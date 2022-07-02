#include "node_pool.h"
#include "page/cell.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/node.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace calico {

NodePool::NodePool(Parameters param)
    : m_free_list {{param.buffer_pool, param.free_start, param.free_count}},
      m_pool {param.buffer_pool},
      m_node_count {param.node_count} {}

auto NodePool::page_size() const -> Size
{
    return m_pool->page_size();
}

auto NodePool::allocate(PageType type) -> Node
{
    auto page = m_free_list.pop();
    if (page) {
        page->set_type(type);
    } else {
        page = m_pool->allocate(type);
    }
    m_node_count++;
    return {std::move(*page), true};
}

auto NodePool::acquire(PID id, bool is_writable) -> Node
{
    return {m_pool->acquire(id, is_writable), false};
}

auto NodePool::destroy(Node node) -> void
{
    CALICO_EXPECT_FALSE(node.is_overflowing());
    m_free_list.push(node.take());
    m_node_count--;
}

auto NodePool::allocate_overflow_chain(BytesView overflow) -> PID
{
    CALICO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = PID::root();

    while (!overflow.is_empty()) {
        auto page = !m_free_list.is_empty()
            ? *m_free_list.pop()
            : m_pool->allocate(PageType::OVERFLOW_LINK);
        page.set_type(PageType::OVERFLOW_LINK);
        Link link {std::move(page)};
        auto content = link.mut_content(std::min(overflow.size(), link.content_size()));
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            prev->set_next_id(link.id());
            prev.reset();
        } else {
            head = link.id();
        }
        prev = std::move(link);
    }
    return head;
}

auto NodePool::collect_overflow_chain(PID id, Bytes out) const -> void
{
    while (!out.is_empty()) {
        auto page = m_pool->acquire(id, false);
        CALICO_EXPECT_EQ(page.type(), PageType::OVERFLOW_LINK);
        Link link {std::move(page)};
        auto content = link.ref_content();
        const auto chunk = std::min(out.size(), content.size());
        mem_copy(out, content, chunk);
        out.advance(chunk);
        id = link.next_id();
    }
}

auto NodePool::destroy_overflow_chain(PID id, Size size) -> void
{
    while (size) {
        auto page = m_pool->acquire(id, true);
        CALICO_EXPECT_EQ(page.type(), PageType::OVERFLOW_LINK); // TODO
        Link link {std::move(page)};
        id = link.next_id();
        size -= std::min(size, link.ref_content().size());
        m_free_list.push(link.take());
    }
}

auto NodePool::save_header(FileHeader &header) -> void
{
    header.set_node_count(m_node_count);
    m_free_list.save_header(header);
}

auto NodePool::load_header(const FileHeader &header) -> void
{
    m_node_count = header.node_count();
    m_free_list.load_header(header);
}

} // calico

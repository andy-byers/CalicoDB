#include "node_pool.h"
#include "page/cell.h"
#include "page/link.h"
#include "page/node.h"
#include "page/page.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

NodePool::NodePool(Parameters param)
    : m_free_list {{param.pager, param.free_start}},
      m_scratch(param.page_size, '\x00'),
      m_pager {param.pager}
{}

auto NodePool::page_size() const -> Size
{
    return m_scratch.size();
}

auto NodePool::allocate(PageType type) -> Result<Node>
{
    auto page = m_free_list.pop()
        .or_else([this](const Status &error) -> Result<Page> {
            if (error.is_logic_error())
                return m_pager->allocate();
            return Err {error};
        });
    if (page) {
        page->set_type(type);
        return Node {std::move(*page), true, m_scratch.data()};
    }
    return Err {page.error()};
}

auto NodePool::acquire(PageId id, bool is_writable) -> Result<Node>
{
    return m_pager->acquire(id, is_writable)
        .and_then([this](Page page) -> Result<Node> {
            return Node {std::move(page), false, m_scratch.data()};
        });
}

auto NodePool::release(Node node) -> Result<void>
{
    CCO_EXPECT_FALSE(node.is_overflowing());
    const auto s = m_pager->release(node.take());
    if (!s.is_ok()) return Err {s}; // TODO: Should return a Status.
    return {};
}

auto NodePool::destroy(Node node) -> Result<void>
{
    CCO_EXPECT_FALSE(node.is_overflowing());
    return m_free_list.push(node.take());
}

auto NodePool::allocate_chain(BytesView overflow) -> Result<PageId>
{
    CCO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = PageId::null();

    while (!overflow.is_empty()) {
        auto page = m_free_list.pop()
            .or_else([this](const Status &error) -> Result<Page> {
                if (error.is_logic_error())
                    return m_pager->allocate();
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
            const auto s = m_pager->release(prev->take());
            if (!s.is_ok()) return Err {s};
        } else {
            head = link.id();
        }
        prev.emplace(std::move(link));
    }
    if (prev) {
        const auto s = m_pager->release(prev->take());
        if (!s.is_ok()) return Err {s};
    }
    return head;
}

auto NodePool::collect_chain(PageId id, Bytes out) const -> Result<void>
{
    while (!out.is_empty()) {
        CCO_TRY_CREATE(page, m_pager->acquire(id, false));
        if (page.type() != PageType::OVERFLOW_LINK) {
            ThreePartMessage message;
            message.set_primary("cannot collect overflow chain");
            message.set_detail("link has an invalid page type 0x{:04X}", static_cast<unsigned>(page.type()));
            return Err {message.corruption()};
        }
        Link link {std::move(page)};
        auto content = link.content_view();
        const auto chunk = std::min(out.size(), content.size());
        mem_copy(out, content, chunk);
        out.advance(chunk);
        id = link.next_id();
        const auto s = m_pager->release(link.take());
        if (!s.is_ok()) return Err {s};
    }
    return {};
}

auto NodePool::destroy_chain(PageId id, Size size) -> Result<void>
{
    while (size) {
        auto page = m_pager->acquire(id, true);
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

auto NodePool::save_state(FileHeader &header) -> void
{
    m_free_list.save_state(header);
}

auto NodePool::load_state(const FileHeader &header) -> void
{
    m_free_list.load_state(header);
}

} // namespace cco

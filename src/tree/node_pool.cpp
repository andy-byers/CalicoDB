#include "node_pool.h"
#include "page/link.h"
#include "page/node.h"
#include "page/page.h"
#include "pager/pager.h"
#include "utils/info_log.h"
#include "utils/layout.h"

namespace calico {

NodePool::NodePool(Pager &pager, Size page_size)
    : m_free_list {pager},
      m_scratch(page_size, '\x00'),
      m_pager {&pager}
{}

auto NodePool::page_size() const -> Size
{
    return m_scratch.size();
}

auto NodePool::page_count() const -> Size
{
    return m_pager->page_count();
}

auto NodePool::allocate(PageType type) -> tl::expected<Node, Status>
{
    auto page = m_free_list.pop()
        .or_else([this](const Status &error) -> tl::expected<Page, Status> {
            if (error.is_logic_error())
                return m_pager->allocate();
            return tl::make_unexpected(error);
        });
    if (page) {
        page->set_type(type);
        return Node {std::move(*page), true, m_scratch.data()};
    }
    return tl::make_unexpected(page.error());
}

auto NodePool::acquire(Id id, bool is_writable) -> tl::expected<Node, Status>
{
    return m_pager->acquire(id, is_writable)
        .and_then([this](Page page) -> tl::expected<Node, Status> {
            return Node {std::move(page), false, m_scratch.data()};
        });
}

auto NodePool::release(Node node) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(node.is_overflowing());
    const auto s = m_pager->release(node.take());
    if (!s.is_ok()) return tl::make_unexpected(s); // TODO: Should return a Status.
    return {};
}

auto NodePool::destroy(Node node) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(node.is_overflowing());
    return m_free_list.push(node.take());
}

auto NodePool::allocate_chain(BytesView overflow) -> tl::expected<Id, Status>
{
    CALICO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = Id::null();

    while (!overflow.is_empty()) {
        auto page = m_free_list.pop()
            .or_else([this](const Status &error) -> tl::expected<Page, Status> {
                if (error.is_logic_error())
                    return m_pager->allocate();
                return tl::make_unexpected(error);
            });
        if (!page.has_value())
            return tl::make_unexpected(page.error());

        page->set_type(PageType::OVERFLOW_LINK);
        Link link {std::move(*page)};
        auto content = link.content_bytes(std::min(overflow.size(), link.content_size()));
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            prev->set_next_id(link.id());
            const auto s = m_pager->release(prev->take());
            if (!s.is_ok()) return tl::make_unexpected(s);
        } else {
            head = link.id();
        }
        prev.emplace(std::move(link));
    }
    if (prev) {
        const auto s = m_pager->release(prev->take());
        if (!s.is_ok()) return tl::make_unexpected(s);
    }
    return head;
}

auto NodePool::collect_chain(Id id, Bytes out) const -> tl::expected<void, Status>
{
    while (!out.is_empty()) {
        CALICO_NEW_R(page, m_pager->acquire(id, false));
        if (page.type() != PageType::OVERFLOW_LINK) {
            ThreePartMessage message;
            message.set_primary("cannot collect overflow chain");
            message.set_detail("link has an invalid page type 0x{:04X}", static_cast<unsigned>(page.type()));
            return tl::make_unexpected(message.corruption());
        }
        Link link {std::move(page)};
        auto content = link.content_view();
        const auto chunk = std::min(out.size(), content.size());
        mem_copy(out, content, chunk);
        out.advance(chunk);
        id = link.next_id();
        const auto s = m_pager->release(link.take());
        if (!s.is_ok()) return tl::make_unexpected(s);
    }
    return {};
}

auto NodePool::destroy_chain(Id id, Size size) -> tl::expected<void, Status>
{
    while (size) {
        auto page = m_pager->acquire(id, true);
        if (!page.has_value())
            return tl::make_unexpected(page.error());
        CALICO_EXPECT_EQ(page->type(), PageType::OVERFLOW_LINK); // TODO: Corruption error, not assertion. Need a logger for this class.
        Link link {std::move(*page)};
        id = link.next_id();
        size -= std::min(size, link.content_view().size());
        CALICO_TRY_R(m_free_list.push(link.take()));
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

} // namespace calico

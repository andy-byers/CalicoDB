#include "node_manager.h"
#include "page/link.h"
#include "page/node.h"
#include "page/page.h"
#include "pager/pager.h"
#include "utils/layout.h"
#include "utils/system.h"

namespace Calico {

NodeManager::NodeManager(Pager &pager, System &system, Size page_size)
    : m_free_list {pager},
      m_scratch(page_size, '\x00'),
      m_pager {&pager},
      m_system {&system}
{}

auto NodeManager::page_size() const -> Size
{
    return m_scratch.size();
}

auto NodeManager::page_count() const -> Size
{
    return m_pager->page_count();
}

auto NodeManager::allocate(PageType type) -> tl::expected<Node__, Status>
{
    auto page = m_free_list.pop()
        .or_else([this](const Status &error) -> tl::expected<Page_, Status> {
            if (error.is_logic_error())
                return m_pager->allocate();
            return tl::make_unexpected(error);
        });
    if (page) {
        page->set_type(type);
        return Node__ {std::move(*page), true, m_scratch.data()};
    }
    CALICO_ERROR(page.error());
    return tl::make_unexpected(page.error());
}

auto NodeManager::acquire(Id id, bool is_writable) -> tl::expected<Node__, Status>
{
    return m_pager->acquire(id, is_writable)
        .and_then([this](Page_ page) -> tl::expected<Node__, Status> {
            return Node__ {std::move(page), false, m_scratch.data()};
        })
        .or_else([this, is_writable](const Status &error) -> tl::expected<Node__, Status> {
            const auto is_severe = is_writable; // || TODO: in transaction?
            m_system->push_error(is_severe ? Error::ERROR : Error::WARN, error);
            return tl::make_unexpected(error);
        });
}

auto NodeManager::release(Node__ node) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(node.is_overflowing());
    const auto was_writable = node.page().is_writable();
    if (auto s = m_pager->release(node.take()); !s.is_ok()) {
        m_system->push_error(was_writable ? Error::ERROR : Error::WARN, s);
        return tl::make_unexpected(s);
    }
    return {};
}

auto NodeManager::destroy(Node__ node) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(node.is_overflowing());
    return m_free_list.push(node.take());
}

auto NodeManager::allocate_chain(Slice overflow) -> tl::expected<Id, Status>
{
    CALICO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = Id::null();

    while (!overflow.is_empty()) {
        auto page = m_free_list.pop()
            .or_else([this](const Status &error) -> tl::expected<Page_, Status> {
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

auto NodeManager::collect_chain(Id id, Span out) const -> tl::expected<void, Status>
{
    while (!out.is_empty()) {
        CALICO_NEW_R(page, m_pager->acquire(id, false));
        if (page.type() != PageType::OVERFLOW_LINK)
            return tl::make_unexpected(corruption(
                "cannot collect overflow chain: link has an invalid page type 0x{:04X}", static_cast<unsigned>(page.type())));

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

auto NodeManager::destroy_chain(Id id, Size size) -> tl::expected<void, Status>
{
    while (size) {
        auto page = m_pager->acquire(id, true);

        if (!page.has_value())
            return tl::make_unexpected(page.error());

        if (page->type() != PageType::OVERFLOW_LINK)
            CALICO_ERROR(corruption("page {} is not an overflow link", page->id().value));

        Link link {std::move(*page)};
        id = link.next_id();
        size -= std::min(size, link.content_view().size());
        CALICO_TRY_R(m_free_list.push(link.take()));
    }
    return {};
}

auto NodeManager::save_state(FileHeader__ &header) -> void
{
    m_free_list.save_state(header);
}

auto NodeManager::load_state(const FileHeader__ &header) -> void
{
    m_free_list.load_state(header);
}

} // namespace Calico

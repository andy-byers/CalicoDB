#include "tree.h"
#include "logging.h"
#include "pager.h"
#include "utils.h"
#include <array>
#include <functional>
#include <numeric>

namespace calicodb
{

static constexpr std::size_t kMaxCellHeaderSize =
    sizeof(std::uint64_t) + // Value size  (varint)
    sizeof(std::uint64_t) + // Key size    (varint)
    sizeof(Id);             // Overflow ID (8 B)

inline constexpr auto compute_min_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 32 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_max_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 64 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_local_size(std::size_t key_size, std::size_t value_size, std::size_t min_local, std::size_t max_local) -> std::size_t
{
    if (key_size + value_size <= max_local) {
        return key_size + value_size;
    } else if (key_size > max_local) {
        return max_local;
    }
    // Try to prevent the key from being split.
    return std::max(min_local, key_size);
}

static auto node_header_offset(const Node &node)
{
    return page_offset(node.page) + TableHeader::kSize * node.is_root;
}

static auto cell_slots_offset(const Node &node)
{
    return node_header_offset(node) + NodeHeader::kSize;
}

static auto cell_area_offset(const Node &node)
{
    return cell_slots_offset(node) + node.header.cell_count * sizeof(PageSize);
}

static auto usable_space(const Node &node) -> std::size_t
{
    return node.header.free_total + node.gap_size;
}

static auto detach_cell(Cell &cell, char *backing) -> void
{
    if (cell.is_free) {
        return;
    }
    std::memcpy(backing, cell.ptr, cell.size);
    const auto diff = cell.key - cell.ptr;
    cell.ptr = backing;
    cell.key = backing + diff;
    cell.is_free = true;
}

static auto read_child_id_at(const Node &node, std::size_t offset) -> Id
{
    return {get_u64(node.page.data() + offset)};
}

static auto write_child_id_at(Node &node, std::size_t offset, Id child_id) -> void
{
    put_u64(node.page.span(offset, sizeof(Id)), child_id.value);
}

static auto read_child_id(const Node &node, std::size_t index) -> Id
{
    const auto &header = node.header;
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        return header.next_id;
    }
    return read_child_id_at(node, node.get_slot(index));
}

static auto read_child_id(const Cell &cell) -> Id
{
    return {get_u64(cell.ptr)};
}

static auto read_overflow_id(const Cell &cell) -> Id
{
    return {get_u64(cell.key + cell.local_size)};
}

static auto write_overflow_id(Cell &cell, Id overflow_id) -> void
{
    put_u64(cell.key + cell.local_size, overflow_id.value);
}

static auto write_child_id(Node &node, std::size_t index, Id child_id) -> void
{
    auto &header = node.header;
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        header.next_id = child_id;
    } else {
        write_child_id_at(node, node.get_slot(index), child_id);
    }
}

static auto write_child_id(Cell &cell, Id child_id) -> void
{
    put_u64(cell.ptr, child_id.value);
}

[[nodiscard]] static auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(page_offset(page)))};
}

static auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(page_offset(page), sizeof(Id)), next_id.value);
}

static auto internal_cell_size(const NodeMeta &meta, const char *data) -> std::size_t
{
    std::size_t key_size;
    const auto *ptr = decode_varint(data + sizeof(Id), key_size);
    const auto local_size = compute_local_size(key_size, 0, meta.min_local, meta.max_local);
    const auto extra_size = (local_size < key_size) * sizeof(Id);
    const auto header_size = static_cast<std::size_t>(ptr - data);
    return header_size + local_size + extra_size;
}

static auto external_cell_size(const NodeMeta &meta, const char *data) -> std::size_t
{
    std::size_t key_size, value_size;
    const auto *ptr = decode_varint(data, value_size);
    ptr = decode_varint(ptr, key_size);
    const auto local_size = compute_local_size(key_size, value_size, meta.min_local, meta.max_local);
    const auto extra_size = (local_size < key_size + value_size) * sizeof(Id);
    const auto header_size = static_cast<std::size_t>(ptr - data);
    return header_size + local_size + extra_size;
}

static auto parse_external_cell(const NodeMeta &meta, char *data) -> Cell
{
    std::size_t key_size, value_size;
    const auto *ptr = decode_varint(data, value_size);
    ptr = decode_varint(ptr, key_size);
    const auto header_size = static_cast<std::size_t>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, value_size, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size + value_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

static auto parse_internal_cell(const NodeMeta &meta, char *data) -> Cell
{
    std::size_t key_size;
    const auto *ptr = data + sizeof(Id);
    ptr = decode_varint(ptr, key_size);
    const auto header_size = static_cast<std::size_t>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, 0, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

static constexpr auto sizeof_meta_lookup() -> std::size_t
{
    std::size_t i {};
    for (std::size_t n {kMinPageSize}; n <= kMaxPageSize; n *= 2) {
        ++i;
    }
    return i;
}

static constexpr auto kMetaLookupSize = sizeof_meta_lookup();

static constexpr auto create_meta_lookup() -> std::array<NodeMeta[2], kMetaLookupSize>
{
    std::array<NodeMeta[2], kMetaLookupSize> lookup;
    for (std::size_t i {}; i < lookup.size(); ++i) {
        const auto page_size = kMinPageSize << i;
        lookup[i][0].min_local = compute_min_local(page_size);
        lookup[i][0].max_local = compute_max_local(page_size);
        lookup[i][0].cell_size = internal_cell_size;
        lookup[i][0].parse_cell = parse_internal_cell;
        lookup[i][1].min_local = compute_min_local(page_size);
        lookup[i][1].max_local = compute_max_local(page_size);
        lookup[i][1].cell_size = internal_cell_size;
        lookup[i][1].parse_cell = parse_internal_cell;
    }
    return lookup;
}

// Stores node-type-specific function pointer lookup tables for every possible page size.
static constexpr auto kMetaLookup = create_meta_lookup();

constexpr auto lookup_meta(std::size_t page_size, bool is_external) -> const NodeMeta *
{
    return &kMetaLookup[page_size >> 10][is_external];
}

[[nodiscard]] static auto cell_size_direct(const Node &node, std::size_t offset) -> std::size_t
{
    return node.meta->cell_size(*node.meta, node.page.data() + offset);
}

class BlockAllocator
{
    Node *m_node {};

    [[nodiscard]] auto get_next_pointer(std::size_t offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset);
    }

    [[nodiscard]] auto get_block_size(std::size_t offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset + sizeof(PageSize));
    }

    auto set_next_pointer(std::size_t offset, PageSize value) -> void
    {
        CDB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.span(offset, sizeof(PageSize)), value);
    }

    auto set_block_size(std::size_t offset, PageSize value) -> void
    {
        CDB_EXPECT_GE(value, 4);
        CDB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.span(offset + sizeof(PageSize), sizeof(PageSize)), value);
    }

    [[nodiscard]] auto allocate_from_free_list(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto allocate_from_gap(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize;

public:
    explicit BlockAllocator(Node &node)
        : m_node {&node}
    {
    }

    [[nodiscard]] auto allocate(PageSize needed_size) -> PageSize;
    auto free(PageSize ptr, PageSize size) -> void;
    auto defragment(std::optional<PageSize> skip = std::nullopt) -> void;
};

auto BlockAllocator::allocate_from_free_list(PageSize needed_size) -> PageSize
{
    PageSize prev_ptr {};
    PageSize curr_ptr {m_node->header.free_start};

    while (curr_ptr) {
        if (needed_size <= get_block_size(curr_ptr)) {
            return take_free_space(prev_ptr, curr_ptr, needed_size);
        }
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(curr_ptr);
    }
    return 0;
}

auto BlockAllocator::allocate_from_gap(PageSize needed_size) -> PageSize
{
    if (needed_size <= m_node->gap_size) {
        m_node->gap_size -= needed_size;
        return m_node->header.cell_start -= needed_size;
    }
    return 0;
}

auto BlockAllocator::take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize
{
    CDB_EXPECT_LT(ptr0, m_node->page.size());
    CDB_EXPECT_LT(ptr1, m_node->page.size());
    CDB_EXPECT_LT(needed_size, m_node->page.size());

    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    auto &header = m_node->header;

    // Caller should make sure it isn't possible to overflow this byte.
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);

    CDB_EXPECT_GE(free_size, needed_size);
    const auto diff = static_cast<PageSize>(free_size - needed_size);

    if (diff < 4) {
        header.frag_count = static_cast<std::uint8_t>(header.frag_count + diff);

        if (is_first) {
            header.free_start = static_cast<PageSize>(ptr2);
        } else {
            set_next_pointer(ptr0, ptr2);
        }
    } else {
        set_block_size(ptr1, diff);
    }
    CDB_EXPECT_GE(header.free_total, needed_size);
    header.free_total -= needed_size;
    return ptr1 + diff;
}

auto BlockAllocator::allocate(PageSize needed_size) -> PageSize
{
    CDB_EXPECT_LT(needed_size, m_node->page.size());

    if (const auto offset = allocate_from_gap(needed_size)) {
        return offset;
    }
    return allocate_from_free_list(needed_size);
}

auto BlockAllocator::free(PageSize ptr, PageSize size) -> void
{
    CDB_EXPECT_GE(ptr, cell_area_offset(*m_node));
    CDB_EXPECT_LE(ptr + size, m_node->page.size());
    auto &header = m_node->header;
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);

    if (size < 4) {
        header.frag_count = static_cast<std::uint8_t>(header.frag_count + size);
    } else {
        set_next_pointer(ptr, header.free_start);
        set_block_size(ptr, size);
        header.free_start = ptr;
    }
    header.free_total += size;
}

auto BlockAllocator::defragment(std::optional<PageSize> skip) -> void
{
    auto &header = m_node->header;
    const auto n = header.cell_count;
    const auto to_skip = skip.has_value() ? *skip : n;
    auto end = static_cast<PageSize>(m_node->page.size());
    auto ptr = m_node->page.data();
    std::vector<PageSize> ptrs(n);

    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        const auto offset = m_node->get_slot(index);
        const auto size = cell_size_direct(*m_node, offset);

        end -= PageSize(size);
        std::memcpy(m_node->scratch + end, ptr + offset, size);
        ptrs[index] = end;
    }
    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        m_node->set_slot(index, ptrs[index]);
    }
    const auto offset = cell_area_offset(*m_node);
    const auto size = m_node->page.size() - offset;
    mem_copy(m_node->page.span(offset, size), {m_node->scratch + offset, size});

    header.cell_start = end;
    header.frag_count = 0;
    header.free_start = 0;
    header.free_total = 0;
    m_node->gap_size = static_cast<PageSize>(end - cell_area_offset(*m_node));
}


static auto setup_node(Node &node) -> void
{
    node.slots_offset = static_cast<PageSize>(cell_slots_offset(node));

    if (node.header.cell_start == 0) {
        node.header.cell_start = static_cast<PageSize>(node.page.size());
    }

    const auto after_header = page_offset(node.page) + NodeHeader::kSize;
    const auto bottom = after_header + node.header.cell_count * sizeof(PageSize);
    const auto top = node.header.cell_start;

    CDB_EXPECT_GE(top, bottom);
    node.gap_size = static_cast<PageSize>(top - bottom);
}

static auto allocate_block(Node &node, PageSize index, PageSize size) -> std::size_t
{
    CDB_EXPECT_LE(index, node.header.cell_count);

    if (size + sizeof(PageSize) > usable_space(node)) {
        node.overflow_index = index;
        return 0;
    }

    BlockAllocator alloc {node};

    // We don't have room to insert the cell pointer.
    if (node.gap_size < sizeof(PageSize)) {
        alloc.defragment(std::nullopt);
    }
    // insert a dummy cell pointer to save the slot.
    node.insert_slot(index, node.page.size() - 1);

    auto offset = alloc.allocate(size);
    if (offset == 0) {
        alloc.defragment(index);
        offset = alloc.allocate(size);
    }
    // We already made sure we had enough room to fulfill the request. If we had to defragment, the call
    // to allocate() following defragmentation should succeed.
    CDB_EXPECT_NE(offset, 0);
    node.set_slot(index, offset);

    // Signal that there will be a change here, but don't write anything yet.
    (void)node.page.span(offset, size);
    return offset;
}

static auto free_block(Node &node, PageSize index, PageSize size) -> void
{
    BlockAllocator alloc {node};
    alloc.free(static_cast<PageSize>(node.get_slot(index)), size);
    node.remove_slot(index);
}

static auto read_cell_at(Node &node, std::size_t offset) -> Cell
{
    return node.meta->parse_cell(*node.meta, node.page.data() + offset);
}

auto read_cell(Node &node, std::size_t index) -> Cell
{
    return read_cell_at(node, node.get_slot(index));
}

auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t
{
    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(cell.size))) {
        auto memory = node.page.span(offset, cell.size);
        std::memcpy(memory.data(), cell.ptr, cell.size);
        return offset;
    }
    node.overflow_index = PageSize(index);
    node.overflow = cell;
    return 0;
}

static auto erase_cell(Node &node, std::size_t index, std::size_t size_hint) -> void
{
    CDB_EXPECT_LT(index, node.header.cell_count);
    free_block(node, PageSize(index), PageSize(size_hint));
}

auto erase_cell(Node &node, std::size_t index) -> void
{
    erase_cell(node, index, cell_size_direct(node, node.get_slot(index)));
}

static auto emplace_cell(char *out, std::size_t key_size, std::size_t value_size, const Slice &local_key, const Slice &local_value, Id overflow_id) -> char *
{
    out = encode_varint(out, value_size);
    out = encode_varint(out, key_size);

    std::memcpy(out, local_key.data(), local_key.size());
    out += local_key.size();

    std::memcpy(out, local_value.data(), local_value.size());
    out += local_value.size();

    if (!overflow_id.is_null()) {
        put_u64(out, overflow_id.value);
        out += sizeof(overflow_id);
    }
    return out;
}

auto manual_defragment(Node &node) -> void
{
    BlockAllocator alloc {node};
    alloc.defragment();
}

auto Node::get_slot(std::size_t index) const -> std::size_t
{
    CDB_EXPECT_LT(index, header.cell_count);
    return get_u16(page.data() + slots_offset + index * sizeof(PageSize));
}

auto Node::set_slot(std::size_t index, std::size_t pointer) -> void
{
    CDB_EXPECT_LT(index, header.cell_count);
    return put_u16(page.span(slots_offset + index * sizeof(PageSize), sizeof(PageSize)), static_cast<PageSize>(pointer));
}

auto Node::insert_slot(std::size_t index, std::size_t pointer) -> void
{
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_GE(gap_size, sizeof(PageSize));
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data + sizeof(PageSize), data, size);
    put_u16(data, static_cast<PageSize>(pointer));

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size -= static_cast<PageSize>(sizeof(PageSize));
    ++header.cell_count;
}

auto Node::remove_slot(std::size_t index) -> void
{
    CDB_EXPECT_LT(index, header.cell_count);
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data, data + sizeof(PageSize), size);

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size += sizeof(PageSize);
    --header.cell_count;
}

auto Node::take() && -> Page
{
    if (page.is_writable()) {
        header.write(page);
    }
    return std::move(page);
}

auto Node::TEST_validate() -> void
{
#if not NDEBUG
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);
    std::vector<char> used(page.size());
    const auto account = [&x = used](auto from, auto size) {
        auto lower = begin(x) + long(from);
        auto upper = begin(x) + long(from) + long(size);
        CDB_EXPECT_FALSE(std::any_of(lower, upper, [](auto byte) {
            return byte != '\x00';
        }));
        std::fill(lower, upper, 1);
    };
    // Header(s) and cell pointers.
    {
        account(0, cell_area_offset(*this));
    }
    // Gap space.
    {
        account(cell_area_offset(*this), gap_size);
    }
    // Free list blocks.
    {
        PageSize i {header.free_start};
        const char *data = page.data();
        std::size_t free_total {};
        while (i) {
            const auto size = get_u16(data + i + sizeof(PageSize));
            account(i, size);
            i = get_u16(data + i);
            free_total += size;
        }
        CDB_EXPECT_EQ(free_total + header.frag_count, header.free_total);
    }
    // Cell bodies. Also makes sure the cells are in order.
    for (std::size_t n {}; n < header.cell_count; ++n) {
        const auto lhs_ptr = get_slot(n);
        const auto lhs_cell = read_cell_at(*this, lhs_ptr);
        account(lhs_ptr, lhs_cell.size);

        if (n + 1 < header.cell_count) {
            const auto rhs_ptr = get_slot(n + 1);
            const auto rhs_cell = read_cell_at(*this, rhs_ptr);
            if (!lhs_cell.has_remote && !rhs_cell.has_remote) {
                Slice lhs_key {lhs_cell.key, lhs_cell.key_size};
                Slice rhs_key {rhs_cell.key, rhs_cell.key_size};
                CDB_EXPECT_LT(lhs_key, rhs_key);
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    const auto total_bytes = std::accumulate(
        begin(used),
        end(used),
        int(header.frag_count),
        [](auto accum, auto next) {
            return accum + next;
        });
    CDB_EXPECT_EQ(page.size(), std::size_t(total_bytes));
#endif // not NDEBUG
}

static auto merge_root(Node &root, Node &child) -> void
{
    CDB_EXPECT_EQ(root.header.next_id, child.page.id());
    const auto &header = child.header;
    if (header.free_total) {
        manual_defragment(child);
    }

    // Copy the cell content area.
    CDB_EXPECT_GE(header.cell_start, FileHeader::kSize + NodeHeader::kSize);
    auto memory = root.page.span(header.cell_start, child.page.size() - header.cell_start);
    std::memcpy(memory.data(), child.page.data() + header.cell_start, memory.size());

    // Copy the header and cell pointers.
    memory = root.page.span(FileHeader::kSize + NodeHeader::kSize, header.cell_count * sizeof(PageSize));
    std::memcpy(memory.data(), child.page.data() + cell_slots_offset(child), memory.size());
    root.header = header;
    root.meta = child.meta;
}

struct SeekResult {
    unsigned index {};
    bool exact {};
};

using FetchKey = std::function<Slice(std::size_t)>;

static auto seek_binary(unsigned n, const Slice &key, const FetchKey &fetch) -> SeekResult
{
    auto upper {n};
    unsigned lower {};

    while (lower < upper) {
        const auto mid = (lower + upper) / 2;
        const auto rhs = fetch(mid);
        if (const auto cmp = key.compare(rhs); cmp < 0) {
            upper = mid;
        } else if (cmp > 0) {
            lower = mid + 1;
        } else {
            return {mid, true};
        }
    }
    return {lower, false};
}

NodeIterator::NodeIterator(Node &node, const Parameters &param)
    : m_pager {param.pager},
      m_lhs_key {param.lhs_key},
      m_rhs_key {param.rhs_key},
      m_node {&node}
{
    CDB_EXPECT_NE(m_pager, nullptr);
    CDB_EXPECT_NE(m_lhs_key, nullptr);
    CDB_EXPECT_NE(m_rhs_key, nullptr);
}

// NOTE: "buffer" is only used if the key is fragmented.
auto NodeIterator::fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status
{
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        out = Slice {cell.key, cell.key_size};
        return Status::ok();
    }

    if (buffer.size() < cell.key_size) {
        buffer.resize(cell.key_size);
    }
    Span key {buffer.data(), cell.key_size};
    mem_copy(key, {cell.key, cell.local_size});
    key.advance(cell.local_size);

    CDB_TRY(OverflowList::read(*m_pager, key, read_overflow_id(cell)));
    out = Slice {buffer}.truncate(cell.key_size);
    return Status::ok();
}

auto NodeIterator::index() const -> std::size_t
{
    return m_index;
}

auto NodeIterator::seek(const Slice &key, bool *found) -> Status
{
    Status s;
    const auto fetch = [&s, this](auto index) {
        Slice out;
        if (s.is_ok()) {
            s = fetch_key(*m_lhs_key, read_cell(*m_node, index), out);
        }
        return out;
    };

    const auto [index, exact] = seek_binary(
        m_node->header.cell_count, key, fetch);

    m_index = index;
    if (found != nullptr) {
        *found = exact;
    }
    return s;
}

auto NodeIterator::seek(const Cell &cell, bool *found) -> Status
{
    if (!cell.has_remote) {
        return seek({cell.key, cell.key_size});
    }
    Slice key;
    CDB_TRY(fetch_key(*m_rhs_key, cell, key));
    return seek(key, found);
}

[[nodiscard]] static auto is_overflowing(const Node &node) -> bool
{
    return node.overflow.has_value();
}

[[nodiscard]] static auto is_underflowing(const Node &node) -> bool
{
    return node.header.cell_count == 0;
}

auto Tree::allocate_tree(Pager &pager, Id *root, Id *freelist_head) -> Status
{
    Node node;
    
    Freelist freelist {pager, freelist_head};
    CDB_TRY(NodeManager::allocate(pager, freelist, &node, nullptr, true));
    *root = node.page.id();
    NodeManager::release(pager, std::move(node));
    
    return Status::ok();
}

auto Tree::node_iterator(Node &node) const -> NodeIterator
{
    const NodeIterator::Parameters param {
        m_pager,
        &m_key_scratch[0],
        &m_key_scratch[1],
    };
    return NodeIterator {node, param};
}

auto Tree::is_pointer_map(Id pid) const -> bool
{
    return PointerMap::lookup(*m_pager, pid) == pid;
}

auto Tree::find(const Slice &key, SearchResult *out) const -> Status
{
    Node root;
    CDB_TRY(acquire(&root, m_root, false));
    return find(key, std::move(root), out);
}

auto Tree::find(const Slice &key, Node node, SearchResult *out) const -> Status
{
    for (;;) {
        bool exact;
        auto itr = node_iterator(node);
        CDB_TRY(itr.seek(key, &exact));

        if (node.header.is_external) {
            out->node = std::move(node);
            out->index = itr.index();
            out->exact = exact;
            return Status::ok();
        }
        const auto next_id = read_child_id(node, itr.index() + exact);
        CDB_EXPECT_NE(next_id, node.page.id()); // Infinite loop.
        release(std::move(node));
        CDB_TRY(acquire(&node, next_id, false));
    }
}

auto Tree::find_parent_id(Id pid, Id *out) const -> Status
{
    PointerMap::Entry entry;
    CDB_TRY(PointerMap::read_entry(*m_pager, pid, &entry));
    *out = entry.back_ptr;
    return Status::ok();
}

auto Tree::fix_parent_id(Id pid, Id parent_id, PointerMap::Type type) -> Status
{
    PointerMap::Entry entry {parent_id, type};
    return PointerMap::write_entry(*m_pager, pid, entry);
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status
{
    if (cell.has_remote) {
        return fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead);
    }
    return Status::ok();
}

auto Tree::insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status
{
    write_cell(node, index, cell);
    if (!node.header.is_external) {
        CDB_TRY(fix_parent_id(read_child_id(cell), node.page.id(), PointerMap::kNode));
    }
    return maybe_fix_overflow_chain(cell, node.page.id());
}

auto Tree::remove_cell(Node &node, std::size_t index) -> Status
{
    const auto cell = read_cell(node, index);
    if (cell.has_remote) {
        CDB_TRY(OverflowList::erase(*m_pager, m_freelist, read_overflow_id(cell)));
    }
    erase_cell(node, index, cell.size);
    return Status::ok();
}

auto Tree::fix_links(Node &node) -> Status
{
    for (std::size_t index {}; index < node.header.cell_count; ++index) {
        const auto cell = read_cell(node, index);
        CDB_TRY(maybe_fix_overflow_chain(cell, node.page.id()));
        if (!node.header.is_external) {
            CDB_TRY(fix_parent_id(read_child_id(cell), node.page.id(), PointerMap::kNode));
        }
    }
    if (!node.header.is_external) {
        CDB_TRY(fix_parent_id(node.header.next_id, node.page.id(), PointerMap::kNode));
    }
    if (node.overflow) {
        CDB_TRY(maybe_fix_overflow_chain(*node.overflow, node.page.id()));
        if (!node.header.is_external) {
            CDB_TRY(fix_parent_id(read_child_id(*node.overflow), node.page.id(), PointerMap::kNode));
        }
    }
    return Status::ok();
}

auto Tree::allocate(Node *out, bool is_external) -> Status
{
    return NodeManager::allocate(*m_pager, m_freelist, out, m_node_scratch.data(), is_external);
}

auto Tree::acquire(Node *out, Id pid, bool upgrade) const -> Status
{
    return NodeManager::acquire(*m_pager, pid, out, m_node_scratch.data(), upgrade);
}

auto Tree::destroy(Node node) -> Status
{
    return NodeManager::destroy(m_freelist, std::move(node));
}

auto Tree::upgrade(Node &node) const -> void
{
    NodeManager::upgrade(*m_pager, node);
}

auto Tree::release(Node node) const -> void
{
    NodeManager::release(*m_pager, std::move(node));
}

auto Tree::resolve_overflow(Node node) -> Status
{
    Node next;
    while (is_overflowing(node)) {
        if (node.page.id().is_root()) {
            CDB_TRY(split_root(std::move(node), next));
        } else {
            CDB_TRY(split_non_root(std::move(node), next));
        }
        node = std::move(next);
    }
    release(std::move(node));
    return Status::ok();
}

auto Tree::split_root(Node root, Node &out) -> Status
{
    Node child;
    CDB_TRY(allocate(&child, root.header.is_external));

    // Copy the cells.
    static constexpr auto kAfterRootHeaders = FileHeader::kSize + NodeHeader::kSize;
    auto data = child.page.span(kAfterRootHeaders, root.page.size() - kAfterRootHeaders);
    mem_copy(data, root.page.view(kAfterRootHeaders, data.size()));

    // Copy the header and cell pointers. Doesn't copy the page LSN.
    child.header = root.header;
    data = child.page.span(NodeHeader::kSize, root.header.cell_count * sizeof(PageSize));
    mem_copy(data, root.page.view(kAfterRootHeaders, data.size()));

    CDB_EXPECT_TRUE(is_overflowing(root));
    std::swap(child.overflow, root.overflow);
    child.overflow_index = root.overflow_index;
    child.gap_size = root.gap_size + FileHeader::kSize;

    root.header = NodeHeader {};
    root.header.is_external = false;
    root.header.next_id = child.page.id();
    setup_node(root);

    CDB_TRY(fix_parent_id(child.page.id(), root.page.id(), PointerMap::kNode));
    release(std::move(root));

    CDB_TRY(fix_links(child));
    out = std::move(child);
    return Status::ok();
}

auto Tree::transfer_left(Node &left, Node &right) -> Status
{
    CDB_EXPECT_EQ(left.header.is_external, right.header.is_external);
    const auto cell = read_cell(right, 0);
    CDB_TRY(insert_cell(left, left.header.cell_count, cell));
    CDB_EXPECT_FALSE(is_overflowing(left));
    erase_cell(right, 0, cell.size);
    return Status::ok();
}

auto Tree::split_non_root(Node right, Node &out) -> Status
{
    CDB_EXPECT_FALSE(right.page.id().is_root());
    CDB_EXPECT_TRUE(is_overflowing(right));
    const auto &header = right.header;

    Id parent_id;
    CDB_TRY(find_parent_id(right.page.id(), &parent_id));
    CDB_EXPECT_FALSE(parent_id.is_null());

    Node parent, left;
    CDB_TRY(acquire(&parent, parent_id, true));
    CDB_TRY(allocate(&left, header.is_external));

    const auto overflow_index = right.overflow_index;
    auto overflow = *right.overflow;
    right.overflow.reset();

    if (overflow_index == header.cell_count) {
        // Note the reversal of the "left" and "right" parameters. We are splitting the other way.
        return split_non_root_fast(
            std::move(parent),
            std::move(right),
            std::move(left),
            overflow,
            out);
    }

    /* Fix the overflow. The overflow cell should fit in either "left" or "right". This routine
     * works by transferring cells, one-by-one, from "right" to "left", and trying to insert the
     * overflow cell. Where the overflow cell is written depends on how many cells we have already
     * transferred. If "overflow_index" is 0, we definitely have enough room in "left". Otherwise,
     * we transfer a cell and try to write the overflow cell to "right". If this isn't possible,
     * then the left node must have enough room, since the maximum cell size is limited to roughly
     * 1/4 of a page. If "right" is more than 3/4 full, then "left" must be less than 1/4 full, so
     * it must be able to accept the overflow cell without overflowing.
     */
    for (std::size_t i {}, n = header.cell_count; i < n; ++i) {
        if (i == overflow_index) {
            CDB_TRY(insert_cell(left, left.header.cell_count, overflow));
            break;
        }
        CDB_TRY(transfer_left(left, right));

        if (usable_space(right) >= overflow.size + 2) {
            CDB_TRY(insert_cell(right, overflow_index - i - 1, overflow));
            break;
        }
        CDB_EXPECT_NE(i + 1, n);
    }
    CDB_EXPECT_FALSE(is_overflowing(left));
    CDB_EXPECT_FALSE(is_overflowing(right));

    auto separator = read_cell(right, 0);
    detach_cell(separator, cell_scratch());

    if (header.is_external) {
        if (!header.prev_id.is_null()) {
            Node left_sibling;
            CDB_TRY(acquire(&left_sibling, header.prev_id, true));
            left_sibling.header.next_id = left.page.id();
            left.header.prev_id = left_sibling.page.id();
            release(std::move(left_sibling));
        }
        right.header.prev_id = left.page.id();
        left.header.next_id = right.page.id();
        CDB_TRY(PayloadManager::promote(*m_pager, m_freelist, nullptr, separator, parent_id));
    } else {
        left.header.next_id = read_child_id(separator);
        CDB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kNode));
        erase_cell(right, 0);
    }

    auto itr = node_iterator(parent);
    CDB_TRY(itr.seek(separator));

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.page.id());
    CDB_TRY(insert_cell(parent, itr.index(), separator));

    release(std::move(left));
    release(std::move(right));
    out = std::move(parent);
    return Status::ok();
}

auto Tree::split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status
{
    const auto &header = left.header;
    CDB_TRY(insert_cell(right, 0, overflow));

    CDB_EXPECT_FALSE(is_overflowing(left));
    CDB_EXPECT_FALSE(is_overflowing(right));

    Cell separator;
    if (header.is_external) {
        if (!header.next_id.is_null()) {
            Node right_sibling;
            CDB_TRY(acquire(&right_sibling, header.next_id, true));
            right_sibling.header.prev_id = right.page.id();
            right.header.next_id = right_sibling.page.id();
            release(std::move(right_sibling));
        }
        right.header.prev_id = left.page.id();
        left.header.next_id = right.page.id();

        separator = read_cell(right, 0);
        CDB_TRY(PayloadManager::promote(*m_pager, m_freelist, cell_scratch(), separator, parent.page.id()));
    } else {
        separator = read_cell(left, header.cell_count - 1);
        detach_cell(separator, cell_scratch());
        erase_cell(left, header.cell_count - 1);

        right.header.next_id = left.header.next_id;
        left.header.next_id = read_child_id(separator);
        CDB_TRY(fix_parent_id(right.header.next_id, right.page.id(), PointerMap::kNode));
        CDB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kNode));
    }

    auto itr = node_iterator(parent);
    CDB_TRY(itr.seek(separator));

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.page.id());
    CDB_TRY(insert_cell(parent, itr.index(), separator));

    const auto offset = !is_overflowing(parent);
    write_child_id(parent, itr.index() + offset, right.page.id());
    CDB_TRY(fix_parent_id(right.page.id(), parent.page.id(), PointerMap::kNode));

    release(std::move(left));
    release(std::move(right));
    out = std::move(parent);
    return Status::ok();
}

auto Tree::resolve_underflow(Node node, const Slice &anchor) -> Status
{
    while (is_underflowing(node)) {
        if (node.page.id().is_root()) {
            return fix_root(std::move(node));
        }
        Id parent_id;
        CDB_TRY(find_parent_id(node.page.id(), &parent_id));
        CDB_EXPECT_FALSE(parent_id.is_null());

        Node parent;
        CDB_TRY(acquire(&parent, parent_id, true));
        // NOTE: Searching for the anchor key from the node we took from should always give us the correct index
        //       due to the B+-tree ordering rules.
        bool exact;
        auto itr = node_iterator(parent);
        CDB_TRY(itr.seek(anchor, &exact));
        CDB_TRY(fix_non_root(std::move(node), parent, itr.index() + exact));
        node = std::move(parent);
    }
    release(std::move(node));
    return Status::ok();
}

auto Tree::internal_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(is_underflowing(left));
    CDB_EXPECT_FALSE(left.header.is_external);
    CDB_EXPECT_FALSE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);

    auto separator = read_cell(parent, index);
    write_cell(left, left.header.cell_count, separator);
    write_child_id(left, left.header.cell_count - 1, left.header.next_id);
    CDB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kNode));
    CDB_TRY(maybe_fix_overflow_chain(separator, left.page.id()));
    erase_cell(parent, index, separator.size);

    while (right.header.cell_count) {
        CDB_TRY(transfer_left(left, right));
    }
    left.header.next_id = right.header.next_id;
    write_child_id(parent, index, left.page.id());
    return Status::ok();
}

auto Tree::external_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(is_underflowing(left));
    CDB_EXPECT_TRUE(left.header.is_external);
    CDB_EXPECT_TRUE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);

    left.header.next_id = right.header.next_id;

    //    const auto separator = read_cell(parent, index);
    //    erase_cell(parent, index, separator.size);
    CDB_TRY(remove_cell(parent, index));

    while (right.header.cell_count) {
        CDB_TRY(transfer_left(left, right));
    }
    write_child_id(parent, index, left.page.id());

    if (!right.header.next_id.is_null()) {
        Node right_sibling;
        CDB_TRY(acquire(&right_sibling, right.header.next_id, true));
        right_sibling.header.prev_id = left.page.id();
        release(std::move(right_sibling));
    }
    return Status::ok();
}

auto Tree::merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status
{
    if (left.header.is_external) {
        CDB_TRY(external_merge_left(left, right, parent, index));
    } else {
        CDB_TRY(internal_merge_left(left, right, parent, index));
    }
    CDB_TRY(fix_links(left));
    return destroy(std::move(right));
}

auto Tree::internal_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(is_underflowing(right));
    CDB_EXPECT_FALSE(left.header.is_external);
    CDB_EXPECT_FALSE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);

    auto separator = read_cell(parent, index);
    write_cell(left, left.header.cell_count, separator);
    write_child_id(left, left.header.cell_count - 1, left.header.next_id);
    CDB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kNode));
    CDB_TRY(maybe_fix_overflow_chain(separator, left.page.id()));
    left.header.next_id = right.header.next_id;

    CDB_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
    write_child_id(parent, index + 1, left.page.id());
    erase_cell(parent, index, separator.size);

    // Transfer the rest of the cells. left shouldn't overflow.
    while (right.header.cell_count) {
        CDB_TRY(transfer_left(left, right));
    }
    return Status::ok();
}

auto Tree::external_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(is_underflowing(right));
    CDB_EXPECT_TRUE(left.header.is_external);
    CDB_EXPECT_TRUE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);

    left.header.next_id = right.header.next_id;
    CDB_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
    write_child_id(parent, index + 1, left.page.id());
    CDB_TRY(remove_cell(parent, index));

    while (right.header.cell_count) {
        CDB_TRY(transfer_left(left, right));
    }
    if (!right.header.next_id.is_null()) {
        Node right_sibling;
        CDB_TRY(acquire(&right_sibling, right.header.next_id, true));
        right_sibling.header.prev_id = left.page.id();
        release(std::move(right_sibling));
    }
    return Status::ok();
}

auto Tree::merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status
{
    if (left.header.is_external) {
        CDB_TRY(external_merge_right(left, right, parent, index));
    } else {
        CDB_TRY(internal_merge_right(left, right, parent, index));
    }
    CDB_TRY(fix_links(left));
    return destroy(std::move(right));
}

auto Tree::fix_non_root(Node node, Node &parent, std::size_t index) -> Status
{
    CDB_EXPECT_FALSE(node.page.id().is_root());
    CDB_EXPECT_TRUE(is_underflowing(node));
    CDB_EXPECT_FALSE(is_overflowing(parent));

    if (index > 0) {
        Node left;
        CDB_TRY(acquire(&left, read_child_id(parent, index - 1), true));
        if (left.header.cell_count == 1) {
            CDB_TRY(merge_right(left, std::move(node), parent, index - 1));
            release(std::move(left));
            CDB_EXPECT_FALSE(is_overflowing(parent));
            return Status::ok();
        }
        CDB_TRY(rotate_right(parent, left, node, index - 1));
        release(std::move(left));
    } else {
        Node right;
        CDB_TRY(acquire(&right, read_child_id(parent, index + 1), true));
        if (right.header.cell_count == 1) {
            CDB_TRY(merge_left(node, std::move(right), parent, index));
            release(std::move(node));
            CDB_EXPECT_FALSE(is_overflowing(parent));
            return Status::ok();
        }
        CDB_TRY(rotate_left(parent, node, right, index));
        release(std::move(right));
    }

    CDB_EXPECT_FALSE(is_overflowing(node));
    release(std::move(node));

    if (is_overflowing(parent)) {
        const auto saved_id = parent.page.id();
        CDB_TRY(resolve_overflow(std::move(parent)));
        CDB_TRY(acquire(&parent, saved_id, true));
    }
    return Status::ok();
}

auto Tree::fix_root(Node root) -> Status
{
    CDB_EXPECT_TRUE(root.page.id().is_root());

    // If the root is external here, the whole tree must be empty.
    if (!root.header.is_external) {
        Node child;
        CDB_TRY(acquire(&child, root.header.next_id, true));

        // We don't have enough room to transfer the child contents into the root, due to the space occupied by
        // the file header. In this case, we'll just split the child and insert the median cell into the root.
        // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
        // extracting an arbitrary cell and making it the overflow cell.
        if (usable_space(child) < FileHeader::kSize) {
            child.overflow_index = child.header.cell_count / 2;
            child.overflow = read_cell(child, child.overflow_index);
            detach_cell(*child.overflow, cell_scratch());
            erase_cell(child, child.overflow_index);
            release(std::move(root));
            Node parent;
            CDB_TRY(split_non_root(std::move(child), parent));
            release(std::move(parent));
            CDB_TRY(acquire(&root, Id::root(), true));
        } else {
            merge_root(root, child);
            CDB_TRY(destroy(std::move(child)));
        }
        CDB_TRY(fix_links(root));
    }
    release(std::move(root));
    return Status::ok();
}

auto Tree::rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    if (left.header.is_external) {
        return external_rotate_left(parent, left, right, index);
    } else {
        return internal_rotate_left(parent, left, right, index);
    }
}

auto Tree::external_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(left.header.is_external);
    CDB_EXPECT_TRUE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);
    CDB_EXPECT_GT(parent.header.cell_count, 0);
    CDB_EXPECT_GT(right.header.cell_count, 1);

    auto lowest = read_cell(right, 0);
    CDB_TRY(insert_cell(left, left.header.cell_count, lowest));
    CDB_EXPECT_FALSE(is_overflowing(left));
    erase_cell(right, 0);

    auto separator = read_cell(right, 0);
    CDB_TRY(PayloadManager::promote(*m_pager, m_freelist, cell_scratch(), separator, parent.page.id()));
    write_child_id(separator, left.page.id());

    CDB_TRY(remove_cell(parent, index));
    return insert_cell(parent, index, separator);
}

auto Tree::internal_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CDB_EXPECT_FALSE(parent.header.is_external);
    CDB_EXPECT_FALSE(left.header.is_external);
    CDB_EXPECT_FALSE(right.header.is_external);
    CDB_EXPECT_GT(parent.header.cell_count, 0);
    CDB_EXPECT_GT(right.header.cell_count, 1);

    Node child;
    CDB_TRY(acquire(&child, read_child_id(right, 0), true));
    const auto saved_id = left.header.next_id;
    left.header.next_id = child.page.id();
    CDB_TRY(fix_parent_id(child.page.id(), left.page.id(), PointerMap::kNode));
    release(std::move(child));

    auto separator = read_cell(parent, index);
    CDB_TRY(insert_cell(left, left.header.cell_count, separator));
    CDB_EXPECT_FALSE(is_overflowing(left));
    write_child_id(left, left.header.cell_count - 1, saved_id);
    erase_cell(parent, index, separator.size);

    auto lowest = read_cell(right, 0);
    detach_cell(lowest, cell_scratch());
    erase_cell(right, 0);
    write_child_id(lowest, left.page.id());
    return insert_cell(parent, index, lowest);
}

auto Tree::rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    if (left.header.is_external) {
        return external_rotate_right(parent, left, right, index);
    } else {
        return internal_rotate_right(parent, left, right, index);
    }
}

auto Tree::external_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(left.header.is_external);
    CDB_EXPECT_TRUE(right.header.is_external);
    CDB_EXPECT_FALSE(parent.header.is_external);
    CDB_EXPECT_GT(parent.header.cell_count, 0);
    CDB_EXPECT_GT(left.header.cell_count, 1);

    auto highest = read_cell(left, left.header.cell_count - 1);
    CDB_TRY(insert_cell(right, 0, highest));
    CDB_EXPECT_FALSE(is_overflowing(right));

    auto separator = highest;
    CDB_TRY(PayloadManager::promote(*m_pager, m_freelist, cell_scratch(), separator, parent.page.id()));
    write_child_id(separator, left.page.id());

    // Don't erase the cell until it has been detached.
    erase_cell(left, left.header.cell_count - 1);

    CDB_TRY(remove_cell(parent, index));
    CDB_TRY(insert_cell(parent, index, separator));
    return Status::ok();
}

auto Tree::internal_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CDB_EXPECT_FALSE(parent.header.is_external);
    CDB_EXPECT_FALSE(left.header.is_external);
    CDB_EXPECT_FALSE(right.header.is_external);
    CDB_EXPECT_GT(parent.header.cell_count, 0);
    CDB_EXPECT_GT(left.header.cell_count, 1);

    Node child;
    CDB_TRY(acquire(&child, left.header.next_id, true));
    const auto child_id = child.page.id();
    CDB_TRY(fix_parent_id(child.page.id(), right.page.id(), PointerMap::kNode));
    left.header.next_id = read_child_id(left, left.header.cell_count - 1);
    release(std::move(child));

    auto separator = read_cell(parent, index);
    CDB_TRY(insert_cell(right, 0, separator));
    CDB_EXPECT_FALSE(is_overflowing(right));
    write_child_id(right, 0, child_id);
    erase_cell(parent, index, separator.size);

    auto highest = read_cell(left, left.header.cell_count - 1);
    detach_cell(highest, cell_scratch());
    write_child_id(highest, left.page.id());
    erase_cell(left, left.header.cell_count - 1, highest.size);
    CDB_TRY(insert_cell(parent, index, highest));
    return Status::ok();
}

Tree::Tree(Pager &pager, Id root, Id *freelist_head)
    : m_node_scratch(pager.page_size(), '\0'),
      m_cell_scratch(pager.page_size(), '\0'),
      m_freelist {pager, freelist_head},
      m_pager {&pager},
      m_root {root}
{
}

auto Tree::cell_scratch() -> char *
{
    // Leave space for a child ID (maximum difference between the size of a varint and an Id).
    return m_cell_scratch.data() + sizeof(Id) - 1;
}

//auto Tree::insert(const Slice &key, const Slice &value, bool *exists) -> Status
//{
//    CDB_EXPECT_FALSE(key.is_empty());
//    Tree internal {*this};
//
//    SearchResult slot;
//    CDB_TRY(internal.find_external_slot(key, slot));
//    auto [node, index, exact] = std::move(slot);
//    internal.upgrade(node);
//
//    if (exact) {
//        CDB_TRY(internal.remove_cell(node, index));
//    }
//
//    CDB_TRY(m_payloads.emplace(cell_scratch(), node, key, value, index));
//    CDB_TRY(internal.resolve_overflow(std::move(node)));
//    if (exists != nullptr) {
//        *exists = exact;
//    }
//    return Status::ok();
//}
//
//auto Tree::erase(const Slice &key) -> Status
//{
//    Tree internal {*this};
//    SearchResult slot;
//
//    CDB_TRY(internal.find_external_slot(key, slot));
//    auto [node, index, exact] = std::move(slot);
//
//    if (exact) {
//        Slice anchor;
//        const auto cell = read_cell(node, index);
//        CDB_TRY(collect_key(m_anchor, cell, anchor));
//
//        internal.upgrade(node);
//        CDB_TRY(internal.remove_cell(node, index));
//        return internal.resolve_underflow(std::move(node), anchor);
//    }
//    internal.release(std::move(node));
//    return Status::not_found("not found");
//}

//auto Tree::lowest(Node &out) -> Status
//{
//    Tree internal {*this};
//    CDB_TRY(internal.acquire(out, Id::root()));
//    while (!out.header.is_external) {
//        const auto next_id = read_child_id(out, 0);
//        internal.release(std::move(out));
//        CDB_TRY(internal.acquire(out, next_id));
//    }
//    return Status::ok();
//}
//
//auto Tree::highest(Node &out) -> Status
//{
//    Tree internal {*this};
//    CDB_TRY(internal.acquire(out, Id::root()));
//    while (!out.header.is_external) {
//        const auto next_id = out.header.next_id;
//        internal.release(std::move(out));
//        CDB_TRY(internal.acquire(out, next_id));
//    }
//    return Status::ok();
//}

//auto Tree::vacuum_step(Page &free, Id last_id) -> Status
//{
//    CDB_EXPECT_NE(free.id(), last_id);
//    Tree internal {*this};
//
//    PointerMap::Entry entry;
//    CDB_TRY(m_pointers.read_entry(last_id, entry));
//
//    const auto fix_basic_link = [&entry, &free, this]() -> Status {
//        Page parent;
//        CDB_TRY(m_pager->acquire(entry.back_ptr, parent));
//        m_pager->upgrade(parent);
//        write_next_id(parent, free.id());
//        m_pager->release(std::move(parent));
//        return Status::ok();
//    };
//
//    switch (entry.type) {
//    case PointerMap::kFreelistLink: {
//        if (last_id == free.id()) {
//
//        } else if (last_id == m_freelist.m_head) {
//            m_freelist.m_head = free.id();
//        } else {
//            // Back pointer points to another freelist page.
//            CDB_EXPECT_FALSE(entry.back_ptr.is_null());
//            CDB_TRY(fix_basic_link());
//            Page last;
//            CDB_TRY(m_pager->acquire(last_id, last));
//            if (const auto next_id = read_next_id(last); !next_id.is_null()) {
//                CDB_TRY(internal.fix_parent_id(next_id, free.id(), PointerMap::kFreelistLink));
//            }
//            m_pager->release(std::move(last));
//        }
//        break;
//    }
//    case PointerMap::kOverflowLink: {
//        // Back pointer points to another overflow chain link, or the head of the chain.
//        CDB_TRY(fix_basic_link());
//        break;
//    }
//    case PointerMap::kOverflowHead: {
//        // Back pointer points to the node that the overflow chain is rooted in. Search through that nodes cells
//        // for the target overflowing cell.
//        Node parent;
//        CDB_TRY(internal.acquire(parent, entry.back_ptr, true));
//        bool found {};
//        for (std::size_t i {}; i < parent.header.cell_count; ++i) {
//            auto cell = read_cell(parent, i);
//            found = cell.has_remote && read_overflow_id(cell) == last_id;
//            if (found) {
//                write_overflow_id(cell, free.id());
//                break;
//            }
//        }
//        CDB_EXPECT_TRUE(found);
//        internal.release(std::move(parent));
//        break;
//    }
//    case PointerMap::kNode: {
//        // Back pointer points to another node. Search through that node for the target child pointer.
//        Node parent;
//        CDB_TRY(internal.acquire(parent, entry.back_ptr, true));
//        CDB_EXPECT_FALSE(parent.header.is_external);
//        bool found {};
//        for (std::size_t i {}; !found && i <= parent.header.cell_count; ++i) {
//            const auto child_id = read_child_id(parent, i);
//            found = child_id == last_id;
//            if (found) {
//                write_child_id(parent, i, free.id());
//            }
//        }
//        CDB_EXPECT_TRUE(found);
//        internal.release(std::move(parent));
//        // Update references.
//        Node last;
//        CDB_TRY(internal.acquire(last, last_id, true));
//        for (std::size_t i {}; i < last.header.cell_count; ++i) {
//            const auto cell = read_cell(last, i);
//            CDB_TRY(internal.maybe_fix_overflow_chain(cell, free.id()));
//            if (!last.header.is_external) {
//                CDB_TRY(internal.fix_parent_id(read_child_id(last, i), free.id()));
//            }
//        }
//        if (!last.header.is_external) {
//            CDB_TRY(internal.fix_parent_id(last.header.next_id, free.id()));
//        } else {
//            if (!last.header.prev_id.is_null()) {
//                Node prev;
//                CDB_TRY(internal.acquire(prev, last.header.prev_id, true));
//                prev.header.next_id = free.id();
//                internal.release(std::move(prev));
//            }
//            if (!last.header.next_id.is_null()) {
//                Node next;
//                CDB_TRY(internal.acquire(next, last.header.next_id, true));
//                next.header.prev_id = free.id();
//                internal.release(std::move(next));
//            }
//        }
//        internal.release(std::move(last));
//    }
//    }
//    CDB_TRY(m_pointers.write_entry(last_id, {}));
//    CDB_TRY(m_pointers.write_entry(free.id(), entry));
//    Page last;
//    CDB_TRY(m_pager->acquire(last_id, last));
//    // We need to upgrade the last node, even though we aren't writing to it. This causes a full image to be written,
//    // which we will need if we crash during vacuum and need to roll back.
//    m_pager->upgrade(last);
//    if (entry.type != PointerMap::kNode) {
//        if (const auto next_id = read_next_id(last); !next_id.is_null()) {
//            PointerMap::Entry next_entry;
//            CDB_TRY(m_pointers.read_entry(next_id, next_entry));
//            next_entry.back_ptr = free.id();
//            CDB_TRY(m_pointers.write_entry(next_id, next_entry));
//        }
//    }
//    mem_copy(free.span(sizeof(Lsn), free.size() - sizeof(Lsn)),
//             last.view(sizeof(Lsn), last.size() - sizeof(Lsn)));
//    m_pager->release(std::move(last));
//    return Status::ok();
//}
//
//auto Tree::vacuum_one(Id target, bool &success) -> Status
//{
//    Tree internal {*this};
//
//    if (internal.is_pointer_map(target)) {
//        success = true;
//        return Status::ok();
//    }
//    if (target.is_root() || m_freelist.is_empty()) {
//        success = false;
//        return Status::ok();
//    }
//
//    // Swap the head of the freelist with the last page in the file.
//    Page head;
//    CDB_TRY(m_freelist.pop(head));
//    if (target != head.id()) {
//        // Swap the last page with the freelist head.
//        CDB_TRY(vacuum_step(head, target));
//    } else {
//        // TODO: May not really be necessary...
//        CDB_TRY(internal.fix_parent_id(target, Id::null(), {}));
//    }
//    m_pager->release(std::move(head));
//    success = true;
//    return Status::ok();
//}

auto Tree::load_state(const FileHeader &header) -> void
{
    *m_freelist.m_head = header.freelist_head;
}

//class TreeValidator
//{
//public:
//    using Callback = std::function<void(Node &, std::size_t)>;
//    using PageCallback = std::function<void(const Page &)>;
//
//    explicit TreeValidator(Tree &tree)
//        : m_tree {&tree}
//    {
//    }
//
//    struct PrintData {
//        std::vector<std::string> levels;
//        std::vector<std::size_t> spaces;
//    };
//
//    auto collect_levels(PrintData &data, Node node, std::size_t level) -> void
//    {
//        Tree internal {*m_tree};
//        const auto &header = node.header;
//        ensure_level_exists(data, level);
//        for (std::size_t cid {}; cid < header.cell_count; ++cid) {
//            const auto is_first = cid == 0;
//            const auto not_last = cid + 1 < header.cell_count;
//            auto cell = read_cell(node, cid);
//
//            if (!header.is_external) {
//                Node next;
//                CHECK_OK(internal.acquire(next, read_child_id(cell), false));
//                collect_levels(data, std::move(next), level + 1);
//            }
//
//            if (is_first) {
//                add_to_level(data, std::to_string(node.page.id().value) + ":[", level);
//            }
//
//            const auto key = Slice {cell.key, std::min<std::size_t>(3, cell.key_size)}.to_string();
//            CHECK_TRUE(!key.empty());
//            add_to_level(data, escape_string(key), level);
//            if (cell.has_remote) {
//                add_to_level(data, "(" + number_to_string(read_overflow_id(cell).value) + ")", level);
//            }
//
//            if (not_last) {
//                add_to_level(data, ",", level);
//            } else {
//                add_to_level(data, "]", level);
//            }
//        }
//        if (!node.header.is_external) {
//            Node next;
//            CHECK_OK(internal.acquire(next, node.header.next_id, false));
//            collect_levels(data, std::move(next), level + 1);
//        }
//
//        internal.release(std::move(node));
//    }
//
//    auto traverse_inorder(const Callback &callback) -> void
//    {
//        Tree internal {*m_tree};
//        Node root;
//        CHECK_OK(internal.acquire(root, Id::root(), false));
//        traverse_inorder_helper(std::move(root), callback);
//    }
//
//    auto validate_freelist(Id head) -> void
//    {
//        Tree internal {*m_tree};
//        auto &pager = *m_pager;
//        auto &freelist = m_freelist;
//        if (freelist.is_empty()) {
//            return;
//        }
//        CHECK_TRUE(!head.is_null());
//        Page page;
//        CHECK_OK(pager.acquire(head, page));
//
//        Id parent_id;
//        traverse_chain(std::move(page), [&](const auto &link) {
//            Id found_id;
//            CHECK_OK(internal.find_parent_id(link.id(), found_id));
//            CHECK_TRUE(found_id == parent_id);
//            parent_id = link.id();
//        });
//    }
//
//    auto validate_overflow(Id overflow_id, Id parent_id, std::size_t overflow_size) -> void
//    {
//        Tree internal {*m_tree};
//        auto &pager = *m_pager;
//
//        Page page;
//        CHECK_OK(pager.acquire(overflow_id, page));
//
//        Id last_id;
//        std::size_t n {};
//        traverse_chain(std::move(page), [&](const auto &link) {
//            Id found_id;
//            CHECK_OK(internal.find_parent_id(link.id(), found_id));
//            CHECK_TRUE(found_id == (n ? last_id : parent_id));
//            n += pager.page_size() - sizeof(Lsn) - sizeof(Id);
//            last_id = link.id();
//        });
//        CHECK_TRUE(n >= overflow_size);
//    }
//
//    auto validate_siblings() -> void
//    {
//        Tree internal {*m_tree};
//
//        const auto validate_possible_overflows = [this](auto &node) {
//            for (std::size_t i {}; i < node.header.cell_count; ++i) {
//                const auto cell = read_cell(node, i);
//                if (cell.has_remote) {
//                    std::size_t value_size;
//                    decode_varint(cell.ptr, value_size);
//                    const auto remote_size = cell.key_size + value_size - cell.local_size;
//                    validate_overflow(read_overflow_id(cell), node.page.id(), remote_size);
//                }
//            }
//        };
//
//        // Find the leftmost external node.
//        Node node;
//        CHECK_OK(internal.acquire(node, Id::root(), false));
//        while (!node.header.is_external) {
//            const auto id = read_child_id(node, 0);
//            internal.release(std::move(node));
//            CHECK_OK(internal.acquire(node, id, false));
//        }
//        std::size_t i {}; // Traverse across the sibling chain to the right.
//        while (!node.header.next_id.is_null()) {
//            ++i;
//            validate_possible_overflows(node);
//            Node right;
//            CHECK_OK(internal.acquire(right, node.header.next_id, false));
//            std::string lhs_buffer, rhs_buffer;
//            Slice lhs_key;
//            CHECK_OK(collect_key(lhs_buffer, read_cell(node, 0), lhs_key));
//            Slice rhs_key;
//            CHECK_OK(collect_key(rhs_buffer, read_cell(right, 0), rhs_key));
//            CHECK_TRUE(lhs_key < rhs_key);
//            CHECK_TRUE(right.header.prev_id == node.page.id());
//            internal.release(std::move(node));
//            node = std::move(right);
//        }
//        validate_possible_overflows(node);
//        internal.release(std::move(node));
//    }
//
//    auto validate_parent_child() -> void
//    {
//        Tree internal {*m_tree};
//        auto check = [&](auto &node, auto index) -> void {
//            Node child;
//            CHECK_OK(internal.acquire(child, read_child_id(node, index), false));
//
//            Id parent_id;
//            CHECK_OK(internal.find_parent_id(child.page.id(), parent_id));
//            CHECK_TRUE(parent_id == node.page.id());
//
//            internal.release(std::move(child));
//        };
//        traverse_inorder([f = std::move(check)](const auto &node, auto index) -> void {
//            const auto count = node.header.cell_count;
//            CHECK_TRUE(index < count);
//
//            if (!node.header.is_external) {
//                f(node, index);
//                // Rightmost child.
//                if (index + 1 == count) {
//                    f(node, index + 1);
//                }
//            }
//        });
//    }
//
//private:
//    auto traverse_inorder_helper(Node node, const Callback &callback) -> void
//    {
//        Tree internal {*m_tree};
//        for (std::size_t index {}; index <= node.header.cell_count; ++index) {
//            if (!node.header.is_external) {
//                const auto saved_id = node.page.id();
//                const auto next_id = read_child_id(node, index);
//
//                // "node" must be released while we traverse, otherwise we are limited in how long of a traversal we can
//                // perform by the number of pager frames.
//                internal.release(std::move(node));
//
//                Node next;
//                CHECK_OK(internal.acquire(next, next_id, false));
//                traverse_inorder_helper(std::move(next), callback);
//
//                CHECK_OK(internal.acquire(node, saved_id, false));
//            }
//            if (index < node.header.cell_count) {
//                callback(node, index);
//            }
//        }
//        internal.release(std::move(node));
//    }
//
//    auto traverse_chain(Page page, const PageCallback &callback) -> void
//    {
//        for (;;) {
//            callback(page);
//
//            const auto next_id = read_next_id(page);
//            m_pager->release(std::move(page));
//            if (next_id.is_null()) {
//                break;
//            }
//            CHECK_OK(m_pager->acquire(next_id, page));
//        }
//    }
//
//    auto add_to_level(PrintData &data, const std::string &message, std::size_t target) -> void
//    {
//        // If target is equal to levels.size(), add spaces to all levels.
//        CHECK_TRUE(target <= data.levels.size());
//        std::size_t i {};
//
//        auto s_itr = begin(data.spaces);
//        auto L_itr = begin(data.levels);
//        while (s_itr != end(data.spaces)) {
//            CHECK_TRUE(L_itr != end(data.levels));
//            if (i++ == target) {
//                // Don't leave trailing spaces. Only add them if there will be more text.
//                L_itr->resize(L_itr->size() + *s_itr, ' ');
//                L_itr->append(message);
//                *s_itr = 0;
//            } else {
//                *s_itr += message.size();
//            }
//            ++L_itr;
//            ++s_itr;
//        }
//    }
//
//    auto ensure_level_exists(PrintData &data, std::size_t level) -> void
//    {
//        while (level >= data.levels.size()) {
//            data.levels.emplace_back();
//            data.spaces.emplace_back();
//        }
//        CHECK_TRUE(data.levels.size() > level);
//        CHECK_TRUE(data.levels.size() == data.spaces.size());
//    }
//
//    Tree *m_tree {};
//};
//
//auto Tree::TEST_to_string() -> std::string
//{
//    std::string repr;
//    TreeValidator::PrintData data;
//    TreeValidator validator {*this};
//    Tree internal {*this};
//
//    Node root;
//    CHECK_OK(internal.acquire(root, Id::root()));
//    validator.collect_levels(data, std::move(root), 0);
//    for (const auto &level : data.levels) {
//        repr.append(level + '\n');
//        //        repr.append(level.substr(0, 200) + '\n');
//    }
//
//    return repr;
//}
//
//auto Tree::TEST_check_order() -> void
//{
//    TreeValidator validator {*this};
//    std::string last_key;
//    auto is_first = true;
//
//    validator.traverse_inorder([&](auto &node, auto index) -> void {
//        std::string buffer;
//        Slice key;
//        CHECK_OK(collect_key(buffer, read_cell(node, index), key));
//        if (is_first) {
//            is_first = false;
//        } else {
//            CHECK_TRUE(!key.is_empty());
//            CHECK_TRUE(last_key <= key);
//        }
//        last_key = key.to_string();
//    });
//}
//
//auto Tree::TEST_check_links() -> void
//{
//    TreeValidator validator {*this};
//    validator.validate_siblings();
//    validator.validate_parent_child();
//    validator.validate_freelist(m_freelist.m_head);
//}
//
//auto Tree::TEST_check_nodes() -> void
//{
//    TreeValidator validator {*this};
//    validator.traverse_inorder([](auto &node, auto index) -> void {
//        // Only validate once per node.
//        if (index == 0) {
//            node.TEST_validate();
//        }
//    });
//}



static constexpr auto kLinkHeaderOffset = sizeof(Lsn);
static constexpr auto kLinkContentOffset = kLinkHeaderOffset + sizeof(Id);

[[nodiscard]] static auto get_readable_content(const Page &page, std::size_t size_limit) -> Slice
{
    return page.view(kLinkContentOffset, std::min(size_limit, page.size() - kLinkContentOffset));
}

[[nodiscard]] static auto get_writable_content(Page &page, std::size_t size_limit) -> Span
{
    return page.span(kLinkContentOffset, std::min(size_limit, page.size() - kLinkContentOffset));
}

auto Freelist::pop(Page &page) -> Status
{
    if (!m_head->is_null()) {
        CDB_TRY(m_pager->acquire(*m_head, page));
        m_pager->upgrade(page, kLinkContentOffset);
        *m_head = read_next_id(page);

        if (!m_head->is_null()) {
            // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
            // node's back pointer at some point.
            const PointerMap::Entry entry {Id::null(), PointerMap::kFreelistLink};
            CDB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
        }
        return Status::ok();
    }
    CDB_EXPECT_TRUE(m_head->is_null());
    return Status::logic_error("free list is empty");
}

auto Freelist::push(Page page) -> Status
{
    CDB_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, *m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry {page.id(), PointerMap::kFreelistLink};
    if (!m_head->is_null()) {
        CDB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    CDB_TRY(PointerMap::write_entry(*m_pager, page.id(), entry));

    *m_head = page.id();
    m_pager->release(std::move(page));
    return Status::ok();
}

// The first pointer map page is always on page 2, right after the root page.
static constexpr Id kFirstMapId {2};

static constexpr auto kEntrySize =
    sizeof(char) + // Type
    sizeof(Id);    // Back pointer

static auto entry_offset(Id map_id, Id pid) -> std::size_t
{
    CDB_EXPECT_GT(pid, map_id);

    // Account for the page LSN.
    return sizeof(Lsn) + (pid.value - map_id.value - 1) * kEntrySize;
}

static auto decode_entry(const char *data) -> PointerMap::Entry
{
    PointerMap::Entry entry;
    entry.type = PointerMap::Type {*data++};
    entry.back_ptr.value = get_u64(data);
    return entry;
}

auto PointerMap::read_entry(Pager &pager, Id pid, Entry *out) -> Status
{
    const auto mid = lookup(pager, pid);
    CDB_EXPECT_GE(mid, kFirstMapId);
    CDB_EXPECT_NE(mid, pid);

    const auto offset = entry_offset(mid, pid);
    CDB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CDB_TRY(pager.acquire(mid, map));
    *out = decode_entry(map.data() + offset);
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::write_entry(Pager &pager, Id pid, Entry entry) -> Status
{
    const auto mid = lookup(pager, pid);
    CDB_EXPECT_GE(mid, kFirstMapId);
    CDB_EXPECT_NE(mid, pid);

    const auto offset = entry_offset(mid, pid);
    CDB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CDB_TRY(pager.acquire(mid, map));
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            pager.upgrade(map);
        }
        auto data = map.span(offset, kEntrySize).data();
        *data++ = entry.type;
        put_u64(data, entry.back_ptr.value);
    }
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::lookup(const Pager &pager, Id pid) -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "pid" is a pointer map
    // page, "pid" will be returned.
    if (pid < kFirstMapId) {
        return Id::null();
    }
    const auto usable_size = pager.page_size() - sizeof(Lsn);
    const auto inc = usable_size / kEntrySize + 1;
    const auto idx = (pid.value - kFirstMapId.value) / inc;
    return {idx * inc + kFirstMapId.value};
}

auto NodeManager::allocate(Pager &pager, Freelist &freelist, Node *out, char *scratch, bool is_external) -> Status
{
    const auto fetch_unused_page = [&freelist, &pager](Page &page) {
        if (freelist.is_empty()) {
            CDB_TRY(pager.allocate(page));
            // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
            // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
            // following it are used.
            if (PointerMap::lookup(pager, page.id()) == page.id()) {
                pager.release(std::move(page));
                CDB_TRY(pager.allocate(page));
            }
            return Status::ok();
        } else {
            return freelist.pop(page);
        }
    };
    CDB_TRY(fetch_unused_page(out->page));
    CDB_EXPECT_NE(PointerMap::lookup(pager, out->page.id()), out->page.id());
    CDB_TRY(pager.allocate(out->page));
    out->header.is_external = is_external;
    out->scratch = scratch;
    out->meta = lookup_meta(out->page.size(), is_external);
    setup_node(*out);
    return Status::ok();
}

auto NodeManager::acquire(Pager &pager, Id pid, Node *out, char *scratch, bool upgrade) -> Status
{
    CDB_TRY(pager.acquire(pid, out->page));
    out->scratch = scratch;
    out->header.read(out->page);
    out->meta = lookup_meta(out->page.size(), out->header.is_external);
    setup_node(*out);
    if (upgrade) {
        pager.upgrade(out->page);
    }
    return Status::ok();
}

auto NodeManager::upgrade(Pager &pager, Node &node) -> void
{
    pager.upgrade(node.page);

    // Ensure that the fragment count byte doesn't overflow. We have to account for the possible addition of
    // 2 fragments.
    if (node.header.frag_count + 6 >= 0xFF) {
        manual_defragment(node);
    }
}

auto NodeManager::release(Pager &pager, Node node) -> void
{
    pager.release(std::move(node).take());
}

auto NodeManager::destroy(Freelist &freelist, Node node) -> Status
{
    return freelist.push(std::move(node).take());
}

auto OverflowList::read(Pager &pager, Span out, Id head_id, std::size_t offset) -> Status
{
    while (!out.is_empty()) {
        Page page;
        CDB_TRY(pager.acquire(head_id, page));
        auto content = get_readable_content(page, page.size());

        if (offset) {
            const auto max = std::min(offset, content.size());
            content.advance(max);
            offset -= max;
        }
        if (!content.is_empty()) {
            const auto size = std::min(out.size(), content.size());
            std::memcpy(out.data(), content.data(), size);
            out.advance(size);
        }
        head_id = read_next_id(page);
        pager.release(std::move(page));
    }
    return Status::ok();
}

auto OverflowList::write(Pager &pager, Freelist &freelist, Id *out, const Slice &first, const Slice &second) -> Status
{
    std::optional<Page> prev;
    auto head = Id::null();
    auto a = first;
    auto b = second;

    if (a.is_empty()) {
        a = b;
        b.clear();
    }

    while (!a.is_empty()) {
        Page page;
        auto s = freelist.pop(page);
        if (s.is_logic_error()) {
            s = pager.allocate(page);
            if (s.is_ok() && PointerMap::lookup(pager, page.id()) == page.id()) {
                pager.release(std::move(page));
                s = pager.allocate(page);
            }
        }
        CDB_TRY(s);

        auto content = get_writable_content(page, a.size() + second.size());
        auto limit = std::min(a.size(), content.size());
        mem_copy(content, a, limit);
        a.advance(limit);

        if (a.is_empty()) {
            a = b;
            b.clear();

            if (!a.is_empty()) {
                content.advance(limit);
                limit = std::min(a.size(), content.size());
                mem_copy(content, a, limit);
                a.advance(limit);
            }
        }
        if (prev) {
            write_next_id(*prev, page.id());
            pager.release(std::move(*prev));
        } else {
            head = page.id();
        }
        prev.emplace(std::move(page));
    }
    if (prev) {
        // "prev" contains the last page in the chain.
        write_next_id(*prev, Id::null());
        pager.release(std::move(*prev));
    }
    *out = head;
    return Status::ok();
}

auto OverflowList::copy(Pager &pager, Freelist &freelist, Id *out, Id overflow_id, std::size_t size) -> Status
{
    std::string scratch; // TODO: Copy page-by-page: no scratch is necessary.
    scratch.resize(size);

    CDB_TRY(read(pager, scratch, overflow_id));
    return write(pager, freelist, out, scratch);
}

auto OverflowList::erase(Pager &pager, Freelist &freelist, Id head_id) -> Status
{
    while (!head_id.is_null()) {
        Page page;
        CDB_TRY(pager.acquire(head_id, page));
        head_id = read_next_id(page);
        pager.upgrade(page);
        CDB_TRY(freelist.push(std::move(page)));
    }
    return Status::ok();
}

auto PayloadManager::emplace(Pager &pager, Freelist &freelist, std::string &scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status
{
    CDB_EXPECT_TRUE(node.header.is_external);

    auto k = key.size();
    auto v = value.size();
    const auto local_size = compute_local_size(k, v, node.meta->min_local, node.meta->max_local);
    const auto has_remote = k + v > local_size;

    if (k > local_size) {
        k = local_size;
        v = 0;
    } else if (has_remote) {
        v = local_size - k;
    }

    CDB_EXPECT_EQ(k + v, local_size);
    auto total_size = local_size + varint_length(key.size()) + varint_length(value.size());

    Id overflow_id;
    if (has_remote) {
        CDB_TRY(OverflowList::write(pager, freelist, &overflow_id, key.range(k), value.range(v)));
        PointerMap::Entry entry {node.page.id(), PointerMap::kOverflowHead};
        CDB_TRY(PointerMap::write_entry(pager, overflow_id, entry));
        total_size += sizeof(overflow_id);
    }

    const auto emplace = [&](auto *out) {
        ::calicodb::emplace_cell(out, key.size(), value.size(), key.range(0, k), value.range(0, v), overflow_id);
    };

    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(total_size))) {
        // Write directly into the node.
        emplace(node.page.data() + offset);
    } else {
        // The node has overflowed. Write the cell to scratch memory.
        emplace(scratch.data());
        node.overflow = parse_external_cell(*node.meta, scratch.data());
        node.overflow->is_free = true;
    }
    return Status::ok();
}

auto PayloadManager::promote(Pager &pager, Freelist &freelist, char *scratch, Cell &cell, Id parent_id) -> Status
{
    detach_cell(cell, scratch);

    // "scratch" should have enough room before its "m_data" member to write the left child ID.
    const auto header_size = sizeof(Id) + varint_length(cell.key_size);
    cell.ptr = cell.key - header_size;
    const auto *meta = lookup_meta(pager.page_size(), true);
    cell.local_size = compute_local_size(cell.key_size, 0, meta->min_local, meta->max_local);
    cell.size = header_size + cell.local_size;
    cell.has_remote = false;

    if (cell.key_size > cell.local_size) {
        // Part of the key is on an overflow page. No value is stored locally in this case, so the local size computation is still correct.
        Id overflow_id;
        CDB_TRY(OverflowList::copy(pager, freelist, &overflow_id, read_overflow_id(cell), cell.key_size - cell.local_size));
        PointerMap::Entry entry {parent_id, PointerMap::kOverflowHead};
        CDB_TRY(PointerMap::write_entry(pager, overflow_id, entry));
        write_overflow_id(cell, overflow_id);
        cell.size += sizeof(Id);
        cell.has_remote = true;
    }
    return Status::ok();
}

auto PayloadManager::collect_key(Pager &pager, std::string &result, const Cell &cell, Slice *key) -> Status
{
    if (result.size() < cell.key_size) {
        result.resize(cell.key_size);
    }
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        mem_copy(result, {cell.key, cell.key_size});
        *key = Slice {result.data(), cell.key_size};
        return Status::ok();
    }
    Span span {result};
    span.truncate(cell.key_size);
    mem_copy(span, {cell.key, cell.local_size});

    CDB_TRY(OverflowList::read(pager, span.range(cell.local_size), read_overflow_id(cell)));
    *key = span.range(0, cell.key_size);
    return Status::ok();
}

auto PayloadManager::collect_value(Pager &pager, std::string &result, const Cell &cell, Slice *value) -> Status
{
    std::size_t value_size;
    decode_varint(cell.ptr, value_size);
    if (result.size() < value_size) {
        result.resize(value_size);
    }
    if (!cell.has_remote) {
        mem_copy(result, {cell.key + cell.key_size, value_size});
        *value = Slice {result.data(), value_size};
        return Status::ok();
    }
    std::size_t remote_key_size {};
    if (cell.key_size > cell.local_size) {
        remote_key_size = cell.key_size - cell.local_size;
    }
    Span span {result};
    span.truncate(value_size);

    if (remote_key_size == 0) {
        const auto local_value_size = cell.local_size - cell.key_size;
        mem_copy(span, {cell.key + cell.key_size, local_value_size});
        span.advance(local_value_size);
    }

    CDB_TRY(OverflowList::read(pager, span, read_overflow_id(cell), remote_key_size));
    *value = Slice {result}.truncate(value_size);
    return Status::ok();
}

#define CHECK_OK(expr)                                                                                                            \
    do {                                                                                                                          \
        if (const auto check_s = (expr); !check_s.is_ok()) {                                                                      \
            std::fprintf(stderr, "error: encountered %s status \"%s\"\n", get_status_name(check_s), check_s.to_string().c_str()); \
            std::abort();                                                                                                         \
        }                                                                                                                         \
    } while (0)

#define CHECK_TRUE(expr)                                              \
    do {                                                              \
        if (!(expr)) {                                                \
            std::fprintf(stderr, "error: \"%s\" was false\n", #expr); \
            std::abort();                                             \
        }                                                             \
    } while (0)

#if CALICODB_BUILD_TESTS

auto Tree::TEST_to_string() -> void
{

}

auto Tree::TEST_validate() -> void
{

}

#else

auto Tree::TEST_to_string() -> void
{
}

auto Tree::TEST_validate() -> void
{
}

#endif // CALICODB_BUILD_TESTS

#undef CHECK_TRUE
#undef CHECK_OK

} // namespace calicodb


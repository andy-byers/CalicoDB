#include "batch_internal.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/utils.h"

namespace cco {

auto BatchInternal::entry_count(const Batch &batch) -> Size
{
    return batch.m_data.size();
}

auto BatchInternal::read_entry(const Batch &batch, Index index) -> Entry
{
    CCO_EXPECT_LT(index, entry_count(batch));
    auto view = stob(batch.m_data[index]);

    Entry entry;
    entry.type = EntryType {view[0]};
    view.advance();

    const auto key_size = get_u16(view);
    view.advance(sizeof(std::uint16_t));

    entry.key = view.range(0, key_size);
    view.advance(key_size);

    entry.value = view;
    return entry;
}

auto BatchInternal::push_entry(Batch &batch, Entry entry) -> void
{
    const auto [type, key, value] = entry;
    const auto entry_size = 3 + key.size() + value.size();
    batch.m_data.emplace_back(entry_size, '\x00');
    auto data = stob(batch.m_data.back());

    data[0] = static_cast<Byte>(type);
    data.advance();

    put_u16(data, static_cast<std::uint16_t>(key.size()));
    data.advance(sizeof(std::uint16_t));

    mem_copy(data, key);
    if (type == EntryType::INSERT) {
        data.advance(key.size());
        mem_copy(data, value);
    }
}

auto Batch::insert(BytesView key, BytesView value) -> void
{
    BatchInternal::Entry entry;
    entry.type = BatchInternal::EntryType::INSERT;
    entry.key = key;
    entry.value = value;
    BatchInternal::push_entry(*this, entry);
}

auto Batch::insert(const std::string &key, const std::string &value) -> void
{
    return insert(stob(key), stob(value));
}

auto Batch::insert(const Record &record) -> void
{
    const auto &[key, value] = record;
    return insert(key, value);
}

auto Batch::erase(BytesView key) -> void
{
    BatchInternal::Entry entry;
    entry.type = BatchInternal::EntryType::ERASE;
    entry.key = key;
    BatchInternal::push_entry(*this, entry);
}

auto Batch::erase(const std::string &key) -> void
{
    return erase(stob(key));
}

auto Batch::append(const Batch &rhs) -> void
{
    m_data.insert(end(m_data), begin(rhs.m_data), end(rhs.m_data));
}

} // cco


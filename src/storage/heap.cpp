#include "heap.h"
#include "utils/expect.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace cco {

namespace fs = std::filesystem;

static auto read_blob_at(const std::string &blob, Bytes &out, Index offset)
{
    Size r {};
    if (auto buffer = stob(blob); offset < buffer.size()) {
        const auto diff = buffer.size() - offset;
        r = std::min(out.size(), diff);
        buffer.advance(offset);
        mem_copy(out, buffer, r);
    }
    out.truncate(r);
    return Status::ok();
}

static auto write_blob_at(std::string &blob, BytesView in, Index offset)
{
    if (const auto write_end = offset + in.size(); blob.size() < write_end)
        blob.resize(write_end);
    mem_copy(stob(blob).range(offset), in);
    return Status::ok();
}

auto RandomAccessHeapReader::read(Bytes &out, Index offset) -> Status
{
    return read_blob_at(*m_blob, out, offset);
}

auto RandomAccessHeapEditor::read(Bytes &out, Index offset) -> Status
{
    return read_blob_at(*m_blob, out, offset);
}

auto RandomAccessHeapEditor::write(BytesView in, Index offset) -> Status 
{
    return write_blob_at(*m_blob, in, offset);
}

auto RandomAccessHeapEditor::sync() -> Status 
{
    return Status::ok();
}

auto AppendHeapWriter::write(BytesView in) -> Status 
{
    return write_blob_at(*m_blob, in, m_blob->size());
}

auto AppendHeapWriter::sync() -> Status 
{
    return Status::ok();
}

auto HeapStorage::open_random_reader(const std::string &name, RandomAccessReader **out) -> Status
{
    if (auto itr = m_files.find(name); itr != end(m_files)) {
        *out = new RandomAccessHeapReader {name, itr->second};
    } else {
        ThreePartMessage message;
        message.set_primary("could not open blob");
        message.set_detail("blob does not exist");
        message.set_hint("open a writer or editor to create the blob");
        return message.not_found();
    }
    return Status::ok();
}

auto HeapStorage::open_random_editor(const std::string &name, RandomAccessEditor **out) -> Status
{
    if (auto itr = m_files.find(name); itr != end(m_files)) {
        *out = new RandomAccessHeapEditor {name, itr->second};
    } else {
        auto [position, truthy] = m_files.emplace(name, std::string {});
        CCO_EXPECT_TRUE(truthy);
        *out = new RandomAccessHeapEditor {name, position->second};
    }
    return Status::ok();
}

auto HeapStorage::open_append_writer(const std::string &name, AppendWriter **out) -> Status
{
    if (auto itr = m_files.find(name); itr != end(m_files)) {
        *out = new AppendHeapWriter {name, itr->second};
    } else {
        auto [position, truthy] = m_files.emplace(name, std::string {});
        CCO_EXPECT_TRUE(truthy);
        *out = new AppendHeapWriter {name, position->second};
    }
    return Status::ok();
}

auto HeapStorage::remove_file(const std::string &name) -> Status
{
    auto itr = m_files.find(name);
    if (itr == end(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not remove blob");
        message.set_detail("blob does not exist");
        return message.system_error();
    }
    m_files.erase(itr);
    return Status::ok();
}

auto HeapStorage::resize_file(const std::string &name, Size size) -> Status
{
    auto itr = m_files.find(name);
    if (itr == end(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not resize blob");
        message.set_detail("blob does not exist");
        return message.system_error();
    }
    itr->second.resize(size);
    return Status::ok();
}

auto HeapStorage::rename_file(const std::string &old_name, const std::string &new_name) -> Status
{
    if (new_name.empty()) {
        ThreePartMessage message;
        message.set_primary("could not rename blob");
        message.set_detail("new name is empty");
        return message.system_error();
    }
    auto node = m_files.extract(old_name);
    if (node.empty()) {
        ThreePartMessage message;
        message.set_primary("cannot rename blob");
        message.set_detail("blob \"{}\" does not exist", old_name);
        return message.system_error();
    }
    node.key() = new_name;
    m_files.insert(std::move(node));
    return Status::ok();
}

auto HeapStorage::file_size(const std::string &name, Size &out) const -> Status
{

    auto itr = m_files.find(name);
    if (itr == cend(m_files)) {
        ThreePartMessage message;
        message.set_primary("cannot get blob size");
        message.set_detail("blob \"{}\" does not exist", name);
        return message.system_error();
    }
    out = itr->second.size();
    return Status::ok();
}

auto HeapStorage::file_exists(const std::string &name) const -> Status
{
    const auto itr = m_files.find(name);
    if (itr == cend(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not find blob");
        message.set_detail("blob \"{}\" does not exist", name);
        return message.not_found();
    }
    return Status::ok();
}

auto HeapStorage::get_file_names(std::vector<std::string> &out) const -> Status
{
    std::transform(cbegin(m_files), cend(m_files), back_inserter(out), [](auto entry) {
        return entry.first;
    });
    return Status::ok();
}

auto HeapStorage::create_directory(const std::string &name) -> Status
{
    CCO_EXPECT_EQ(m_directories.find(name), cend(m_directories));
    // Just keep track of nested directory names for now.
    m_directories.insert(name);
    return Status::ok();
}

auto HeapStorage::remove_directory(const std::string &name) -> Status
{
    CCO_EXPECT_NE(m_directories.find(name), cend(m_directories));
    m_directories.erase(name);
    return Status::ok();
}

} // namespace cco
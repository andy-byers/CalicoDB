#ifndef CCO_TEST_TOOLS_FAKES_H
#define CCO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "calico/storage.h"
#include "pool/interface.h"
#include "random.h"
#include "storage/heap.h"
#include <filesystem>
#include <gmock/gmock.h>

namespace cco {

class MockRandomAccessReader: public RandomAccessReader {
public:
    ~MockRandomAccessReader() override = default;
    MOCK_METHOD(Status, read, (Bytes&, Index), (override));

    explicit MockRandomAccessReader(RandomAccessReader *real)
        : m_real {real}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, read).WillByDefault([this](Bytes &out, Index offset) {
            return m_real->read(out, offset);
        });
    }

private:
    std::unique_ptr<RandomAccessReader> m_real;
};

class MockRandomAccessEditor: public RandomAccessEditor {
public:
    ~MockRandomAccessEditor() override = default;
    MOCK_METHOD(Status, read, (Bytes&, Index), (override));
    MOCK_METHOD(Status, write, (BytesView, Index), (override));
    MOCK_METHOD(Status, sync, (), (override));

    explicit MockRandomAccessEditor(RandomAccessEditor *real)
        : m_real {real}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, read).WillByDefault([this](Bytes &out, Index offset) {
            return m_real->read(out, offset);
        });
        ON_CALL(*this, write).WillByDefault([this](BytesView in, Index offset) {
            return m_real->write(in, offset);
        });
        ON_CALL(*this, sync).WillByDefault([this] {
            return m_real->sync();
        });
    }

private:
    std::unique_ptr<RandomAccessEditor> m_real;
};

class MockAppendWriter: public AppendWriter {
public:
    ~MockAppendWriter() override = default;
    MOCK_METHOD(Status, write, (BytesView), (override));
    MOCK_METHOD(Status, sync, (), (override));

    explicit MockAppendWriter(AppendWriter *real)
        : m_real {real}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, write).WillByDefault([this](BytesView in) {
            return m_real->write(in);
        });
        ON_CALL(*this, sync).WillByDefault([this] {
            return m_real->sync();
        });
    }

private:
    std::unique_ptr<AppendWriter> m_real;
};

template<class Blob>
struct BlobTag {};

template<>
struct BlobTag<RandomAccessReader> {
    static constexpr auto value = "RaRe";
};

template<>
struct BlobTag<RandomAccessEditor> {
    static constexpr auto value = "RaEd";
};

template<>
struct BlobTag<AppendWriter> {
    static constexpr auto value = "ApWr";
};

template<class BlobAccessor>
auto get_blob_descriptor(const std::string &name) -> std::string
{
    // Filenames cannot contain '@' during testing.
    return BlobTag<BlobAccessor>::value + std::string {"@"} + name;
}

class MockStorage: public Storage {
public:
    ~MockStorage() override = default;
    MOCK_METHOD(Status, get_blob_names, (std::vector<std::string>&), (const, override));
    MOCK_METHOD(Status, open_random_access_reader, (const std::string &, RandomAccessReader**), (override));
    MOCK_METHOD(Status, open_random_access_editor, (const std::string &, RandomAccessEditor**), (override));
    MOCK_METHOD(Status, open_append_writer, (const std::string &, AppendWriter**), (override));
    MOCK_METHOD(Status, rename_blob, (const std::string &, const std::string &), (override));
    MOCK_METHOD(Status, blob_exists, (const std::string &), (const, override));
    MOCK_METHOD(Status, blob_size, (const std::string &, Size &), (const, override));
    MOCK_METHOD(Status, remove_blob, (const std::string &), (override));

    MockStorage() = default;

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, get_blob_names).WillByDefault([this](std::vector<std::string> &out) {
            return m_real.get_blob_names(out);
        });
        ON_CALL(*this, open_random_access_reader).WillByDefault([this](const std::string &name, RandomAccessReader **out) {
            register_mock<RandomAccessReader, MockRandomAccessReader>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, open_random_access_editor).WillByDefault([this](const std::string &name, RandomAccessEditor **out) {
            register_mock<RandomAccessEditor, MockRandomAccessEditor>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, open_append_writer).WillByDefault([this](const std::string &name, AppendWriter **out) {
            register_mock<AppendWriter, MockAppendWriter>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, rename_blob).WillByDefault([this](const std::string &old_name, const std::string &new_name) {
            const auto maybe_rename_mock = [this](const auto &old_name, const auto &new_name) {
                auto node = m_mocks.extract(old_name);
                if (!node.empty()) {
                    node.key() = new_name;
                    m_mocks.insert(std::move(node));
                }
            };
            auto s = m_real.rename_blob(old_name, new_name);
            if (s.is_ok()) {
                maybe_rename_mock(get_blob_descriptor<RandomAccessReader>(old_name), get_blob_descriptor<RandomAccessReader>(new_name));
                maybe_rename_mock(get_blob_descriptor<RandomAccessEditor>(old_name), get_blob_descriptor<RandomAccessEditor>(new_name));
                maybe_rename_mock(get_blob_descriptor<AppendWriter>(old_name), get_blob_descriptor<AppendWriter>(new_name));
            }
            return s;
        });
        ON_CALL(*this, blob_exists).WillByDefault([this](const std::string &name) {
            return m_real.blob_exists(name);
        });
        ON_CALL(*this, blob_size).WillByDefault([this](const std::string &name, Size &out) {
            return m_real.blob_size(name, out);
        });
        ON_CALL(*this, remove_blob).WillByDefault([this](const std::string &name) {
            auto s = m_real.remove_blob(name);
            if (s.is_ok()) {
                m_mocks.erase(get_blob_descriptor<RandomAccessReader>(name));
                m_mocks.erase(get_blob_descriptor<RandomAccessEditor>(name));
                m_mocks.erase(get_blob_descriptor<AppendWriter>(name));
            }
            return s;
        });
    }

    auto get_mock_random_access_reader(const std::string &name) -> MockRandomAccessReader*
    {
        const auto descriptor = get_blob_descriptor<RandomAccessReader>(name);
        auto itr = m_mocks.find(descriptor);
        return itr != cend(m_mocks) ? static_cast<MockRandomAccessReader *>(itr->second) : nullptr;
    }

    auto get_mock_random_access_editor(const std::string &name) -> MockRandomAccessEditor*
    {
        const auto descriptor = get_blob_descriptor<RandomAccessEditor>(name);
        auto itr = m_mocks.find(descriptor);
        return itr != cend(m_mocks) ? static_cast<MockRandomAccessEditor *>(itr->second) : nullptr;
    }

    auto get_mock_append_writer(const std::string &name) -> MockAppendWriter*
    {
        const auto descriptor = get_blob_descriptor<AppendWriter>(name);
        auto itr = m_mocks.find(descriptor);
        return itr != cend(m_mocks) ? static_cast<MockAppendWriter *>(itr->second) : nullptr;
    }

private:
    template<class Base, class Mock>
    auto register_mock(const std::string &name, Base **out) -> void
    {
        const auto descriptor = get_blob_descriptor<RandomAccessReader>(name);
        CCO_EXPECT_EQ(m_mocks.find(descriptor), cend(m_mocks));

        auto s = Status::ok();
        Base *base {};

        // We need to use the real object to open blobs. All blobs with the same name share memory.
        if constexpr (std::is_same_v<Base, RandomAccessReader>) {
            s = m_real.open_random_access_reader(name, &base);
        } else if constexpr (std::is_same_v<Base, RandomAccessEditor>) {
            s = m_real.open_random_access_editor(name, &base);
        } else if constexpr (std::is_same_v<Base, AppendWriter>) {
            s = m_real.open_append_writer(name, &base);
        } else {
            CCO_EXPECT_TRUE(false && "Unexpected base class for mock");
        }
        CCO_EXPECT_OK(s);

        // The mock blob reader takes ownership of the real object.
        auto *mock = new Mock {base};
        m_mocks.emplace(descriptor, mock);
        *out = mock;
    }

    template<class Base, class Mock>
    auto lookup_mock(const std::string &name) -> Mock*
    {
        auto itr = m_mocks.find(get_blob_descriptor<RandomAccessReader>(name));
        return itr != cend(m_mocks) ? itr->second : nullptr;
    }

    std::unordered_map<std::string, void*> m_mocks;
    HeapStorage m_real;
};

} // cco

#endif // CCO_TEST_TOOLS_FAKES_H

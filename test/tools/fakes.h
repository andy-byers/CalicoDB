#ifndef CALICO_TEST_TOOLS_FAKES_H
#define CALICO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "calico/storage.h"
#include "pager/pager.h"
#include "random.h"
#include "store/heap.h"
#include <filesystem>
#include <gmock/gmock.h>

namespace calico {

class MockRandomReader: public RandomReader {
public:
    ~MockRandomReader() override = default;
    MOCK_METHOD(Status, read, (Bytes&, Size), (override));

    explicit MockRandomReader(RandomReader *real)
        : m_real {real}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, read).WillByDefault([this](Bytes &out, Size offset) {
            return m_real->read(out, offset);
        });
    }

private:
    std::unique_ptr<RandomReader> m_real;
};

class MockRandomEditor: public RandomEditor {
public:
    ~MockRandomEditor() override = default;
    MOCK_METHOD(Status, read, (Bytes&, Size), (override));
    MOCK_METHOD(Status, write, (BytesView, Size), (override));
    MOCK_METHOD(Status, sync, (), (override));

    explicit MockRandomEditor(RandomEditor *real)
        : m_real {real}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, read).WillByDefault([this](Bytes &out, Size offset) {
            return m_real->read(out, offset);
        });
        ON_CALL(*this, write).WillByDefault([this](BytesView in, Size offset) {
            return m_real->write(in, offset);
        });
        ON_CALL(*this, sync).WillByDefault([this] {
            return m_real->sync();
        });
    }

private:
    std::unique_ptr<RandomEditor> m_real;
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

static constexpr Size BLOB_TAG_WIDTH {4};

template<class Blob>
struct BlobTag {};

template<>
struct BlobTag<RandomReader> {
    static constexpr auto value = "RaRe";
};

template<>
struct BlobTag<RandomEditor> {
    static constexpr auto value = "RaEd";
};

template<>
struct BlobTag<AppendWriter> {
    static constexpr auto value = "ApWr";
};

template<class BlobAccessor>
auto get_blob_descriptor(const std::string &path) -> std::string
{
    // File paths cannot contain '@' during testing.
    return BlobTag<BlobAccessor>::value + std::string {"@"} + path;
}

inline auto split_blob_descriptor(const std::string &descriptor) -> std::pair<std::string, std::string>
{
    auto bytes = stob(descriptor);
    return {btos(bytes.range(0, BLOB_TAG_WIDTH)), btos(bytes.range(BLOB_TAG_WIDTH))};
}

class MockStorage: public Storage {
public:
    ~MockStorage() override = default;
    MOCK_METHOD(Status, create_directory, (const std::string &), (override));
    MOCK_METHOD(Status, remove_directory, (const std::string &), (override));
    MOCK_METHOD(Status, get_children, (const std::string &, std::vector<std::string> &), (const, override));
    MOCK_METHOD(Status, open_random_reader, (const std::string &, RandomReader**), (override));
    MOCK_METHOD(Status, open_random_editor, (const std::string &, RandomEditor**), (override));
    MOCK_METHOD(Status, open_append_writer, (const std::string &, AppendWriter**), (override));
    MOCK_METHOD(Status, rename_file, (const std::string &, const std::string &), (override));
    MOCK_METHOD(Status, resize_file, (const std::string &, Size), (override));
    MOCK_METHOD(Status, file_exists, (const std::string &), (const, override));
    MOCK_METHOD(Status, file_size, (const std::string &, Size &), (const, override));
    MOCK_METHOD(Status, remove_file, (const std::string &), (override));

    explicit MockStorage()
        : m_real {std::make_unique<HeapStorage>()}
    {}

    auto delegate_to_real() -> void
    {
        ON_CALL(*this, create_directory).WillByDefault([this](const std::string &name) {
            return m_real->create_directory(name);
        });
        ON_CALL(*this, remove_directory).WillByDefault([this](const std::string &name) {
            return m_real->remove_directory(name);
        });
        ON_CALL(*this, get_children).WillByDefault([this](const std::string &path, std::vector<std::string> &out) {
            return m_real->get_children(path, out);
        });
        ON_CALL(*this, open_random_reader).WillByDefault([this](const std::string &name, RandomReader **out) {
            register_mock<RandomReader, MockRandomReader>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, open_random_editor).WillByDefault([this](const std::string &name, RandomEditor **out) {
            register_mock<RandomEditor, MockRandomEditor>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, open_append_writer).WillByDefault([this](const std::string &name, AppendWriter **out) {
            register_mock<AppendWriter, MockAppendWriter>(name, out);
            return Status::ok();
        });
        ON_CALL(*this, rename_file).WillByDefault([this](const std::string &old_name, const std::string &new_name) {
            const auto maybe_rename_mock = [this](const auto &old_name, const auto &new_name) {
                auto node = m_mocks.extract(old_name);
                if (!node.empty()) {
                    node.key() = new_name;
                    m_mocks.insert(std::move(node));
                }
            };
            auto s = m_real->rename_file(old_name, new_name);
            if (s.is_ok()) {
                maybe_rename_mock(get_blob_descriptor<RandomReader>(old_name), get_blob_descriptor<RandomReader>(new_name));
                maybe_rename_mock(get_blob_descriptor<RandomEditor>(old_name), get_blob_descriptor<RandomEditor>(new_name));
                maybe_rename_mock(get_blob_descriptor<AppendWriter>(old_name), get_blob_descriptor<AppendWriter>(new_name));
            }
            return s;
        });
        ON_CALL(*this, file_exists).WillByDefault([this](const std::string &name) {
            return m_real->file_exists(name);
        });
        ON_CALL(*this, file_size).WillByDefault([this](const std::string &name, Size &out) {
            return m_real->file_size(name, out);
        });
        ON_CALL(*this, resize_file).WillByDefault([this](const std::string &name, Size size) {
            return m_real->resize_file(name, size);
        });
        ON_CALL(*this, remove_file).WillByDefault([this](const std::string &name) {
            auto s = m_real->remove_file(name);
            if (s.is_ok()) {
                m_mocks.erase(get_blob_descriptor<RandomReader>(name));
                m_mocks.erase(get_blob_descriptor<RandomEditor>(name));
                m_mocks.erase(get_blob_descriptor<AppendWriter>(name));
            }
            return s;
        });
    }

    auto clone() -> Storage*
    {
        auto *mock = new MockStorage;
        mock->m_mocks = m_mocks;
        auto *temp = dynamic_cast<HeapStorage*>(m_real->clone());
        mock->m_real.reset(temp);
        for (const auto &[descriptor, unused]: m_mocks) {
            const auto [tag, name] = split_blob_descriptor(descriptor);
            if (tag == BlobTag<RandomReader>::value) {
                RandomReader *file {};
                mock->open_random_reader(name, &file);
                delete file;
            } else if (tag == BlobTag<RandomEditor>::value) {
                RandomEditor *file {};
                mock->open_random_editor(name, &file);
                delete file;
            } else if (tag == BlobTag<AppendWriter>::value) {
                AppendWriter *file {};
                mock->open_append_writer(name, &file);
                delete file;
            }
        }
        return mock;
    }

    [[nodiscard]]
    auto get_mock_random_reader(const std::string &name) -> MockRandomReader*
    {
        const auto descriptor = get_blob_descriptor<RandomReader>(name);
        auto itr = m_mocks.find(descriptor);
        return itr != cend(m_mocks) ? static_cast<MockRandomReader *>(itr->second) : nullptr;
    }

    [[nodiscard]]
    auto get_mock_random_editor(const std::string &name) -> MockRandomEditor*
    {
        const auto descriptor = get_blob_descriptor<RandomEditor>(name);
        auto itr = m_mocks.find(descriptor);
        return itr != cend(m_mocks) ? static_cast<MockRandomEditor *>(itr->second) : nullptr;
    }

    [[nodiscard]]
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
        const auto descriptor = get_blob_descriptor<Base>(name);
        CALICO_EXPECT_EQ(m_mocks.find(descriptor), cend(m_mocks));

        auto s = Status::ok();
        Base *base {};

        // We need to use the real object to open blobs. All blobs with the same name share memory.
        if constexpr (std::is_same_v<Base, RandomReader>) {
            s = m_real->open_random_reader(name, &base);
        } else if constexpr (std::is_same_v<Base, RandomEditor>) {
            s = m_real->open_random_editor(name, &base);
        } else if constexpr (std::is_same_v<Base, AppendWriter>) {
            s = m_real->open_append_writer(name, &base);
        } else {
            CALICO_EXPECT_TRUE(false && "Unexpected base class for mock");
        }
        CALICO_EXPECT_OK(s);

        // The mock blob reader takes ownership of the real object.
        auto *mock = new Mock {base};
        mock->delegate_to_real();
        m_mocks.emplace(descriptor, mock);
        *out = mock;
    }

    template<class Base, class Mock>
    auto lookup_mock(const std::string &name) -> Mock*
    {
        auto itr = m_mocks.find(get_blob_descriptor<RandomReader>(name));
        return itr != cend(m_mocks) ? itr->second : nullptr;
    }

    std::unordered_map<std::string, void*> m_mocks;
    std::unique_ptr<HeapStorage> m_real;
};

} // cco

#endif // CALICO_TEST_TOOLS_FAKES_H

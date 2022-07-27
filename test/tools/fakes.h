#ifndef CCO_TEST_TOOLS_FAKES_H
#define CCO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "pool/interface.h"
#include "random.h"
#include "storage/interface.h"
#include <filesystem>
#include <gmock/gmock.h>

namespace cco {

class FakeFile;
class MockFile;

class SharedMemory {
public:
    SharedMemory()
        : m_memory {std::make_shared<std::string>()} {}

    [[nodiscard]] auto memory() -> std::string&
    {
        return *m_memory;
    }

    [[nodiscard]] auto memory() const -> const std::string&
    {
        return *m_memory;
    }

private:
    std::shared_ptr<std::string> m_memory;
};

class FaultControls {
public:
    struct Controls {
        // TODO: Corrupted reads and writes.
        unsigned read_fault_rate {};
        unsigned write_fault_rate {};
        int read_fault_counter {-1};
        int write_fault_counter {-1};
    };

    FaultControls():
          m_controls {std::make_shared<Controls>()} {}

    explicit FaultControls(std::shared_ptr<Controls> controls):
          m_controls {std::move(controls)} {}

    [[nodiscard]] auto controls() -> Controls&
    {
        return *m_controls;
    }

    [[nodiscard]] auto controls() const -> const Controls&
    {
        return *m_controls;
    }

    [[nodiscard]] auto read_fault_rate() const
    {
        return m_controls->read_fault_rate;
    }

    [[nodiscard]] auto write_fault_rate() const
    {
        return m_controls->write_fault_rate;
    }

    [[nodiscard]] auto read_fault_counter() const
    {
        return m_controls->read_fault_counter;
    }

    [[nodiscard]] auto write_fault_counter() const
    {
        return m_controls->write_fault_counter;
    }

    auto set_read_fault_rate(unsigned rate)
    {
        CCO_EXPECT_LE(rate, 100);
        m_controls->read_fault_rate = rate;
    }

    auto set_write_fault_rate(unsigned rate)
    {
        CCO_EXPECT_LE(rate, 100);
        m_controls->write_fault_rate = rate;
    }

    auto set_read_fault_counter(int value)
    {
        CCO_EXPECT_GE(value, -1);
        m_controls->read_fault_counter = value;
    }

    auto set_write_fault_counter(int value)
    {
        CCO_EXPECT_GE(value, -1);
        m_controls->write_fault_counter = value;
    }

private:
    std::shared_ptr<Controls> m_controls;
};

class FakeDirectory : public IDirectory {
public:
    ~FakeDirectory() override = default;
    explicit FakeDirectory(const std::string&);
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto children() const -> Result<std::vector<std::string>> override;
    [[nodiscard]] auto open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> override;
    [[nodiscard]] auto remove() -> Result<void> override;
    [[nodiscard]] auto sync() -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;

    [[nodiscard]] auto is_open() const -> bool override
    {
        return m_is_open;
    }

    [[nodiscard]] auto exists(const std::string &name) const -> Result<bool> override
    {
        return m_faults.find(name) != end(m_faults);
    }

    [[nodiscard]] auto open_fake_file(const std::string&, Mode, int) -> std::unique_ptr<FakeFile>;

    [[nodiscard]] auto get_shared(const std::string &name) -> SharedMemory
    {
        auto itr = m_shared.find(name);
        CCO_EXPECT_NE(itr, end(m_shared));
        return itr->second;
    }

    [[nodiscard]] auto get_faults(const std::string &name) -> FaultControls
    {
        auto itr = m_faults.find(name);
        CCO_EXPECT_NE(itr, end(m_faults));
        return itr->second;
    }

private:
    std::unordered_map<std::string, SharedMemory> m_shared;
    std::unordered_map<std::string, FaultControls> m_faults;
    std::filesystem::path m_path;
    bool m_is_open {true};
};

class FakeFile : public IFile {
public:
    ~FakeFile() override = default;

    explicit FakeFile(const std::string &path):
          m_path {path} {}

    FakeFile(const std::string &path, SharedMemory memory, FaultControls faults):
          m_faults {std::move(faults)},
          m_memory {std::move(memory)},
          m_path {path} {}

    [[nodiscard]] auto is_open() const -> bool override
    {
        return m_is_open;
    }

    [[nodiscard]] auto mode() const -> Mode override
    {
        return {}; // TODO: Store mode and return here.
    }

    [[nodiscard]] auto permissions() const -> int override
    {
        return m_permissions;
    }

    [[nodiscard]] auto file() const -> int override
    {
        return -1;
    }

    [[nodiscard]] auto path() const -> std::string override
    {
        return m_path;
    }

    [[nodiscard]] auto name() const -> std::string override
    {
        return m_path.filename();
    }

    [[nodiscard]] auto faults() -> FaultControls
    {
        return m_faults;
    }

    [[nodiscard]] auto shared_memory() -> SharedMemory
    {
        return m_memory;
    }

    [[nodiscard]] auto random() -> Random&
    {
        return m_random;
    }

    [[nodiscard]] auto memory() const -> const std::string&
    {
        return m_memory.memory();
    }

    [[nodiscard]] auto memory() -> std::string&
    {
        return m_memory.memory();
    }

    [[nodiscard]] auto cursor() const -> const Index&
    {
        return m_cursor;
    }

    [[nodiscard]] auto cursor() -> Index&
    {
        return m_cursor;
    }

    [[nodiscard]] auto size() const -> Result<Size> override
    {
        return m_memory.memory().size();
    }

    [[nodiscard]] auto open(const std::string &path, Mode mode, int permissions) -> Result<void> override
    {
        m_is_open = true;
        m_path = path;
        m_permissions = permissions;
        m_mode = mode;
        m_is_append = int(mode) & int(Mode::APPEND);
        return {};
    }

    [[nodiscard]] auto close() -> Result<void> override
    {
        m_is_open = false;
        return {};
    }

    // TODO: Shared memory filename?
    [[nodiscard]] auto rename(const std::string&) -> Result<void> override
    {
        return {};
    }

    [[nodiscard]] auto remove() -> Result<void> override
    {
        m_path.clear();
        return {};
    }

    auto seek(long, Seek) -> Result<Index> override;
    auto read(Bytes) -> Result<Size> override;
    auto read(Bytes, Index) -> Result<Size> override;
    auto write(BytesView) -> Result<Size> override;
    auto write(BytesView, Index) -> Result<Size> override;
    auto sync() -> Result<void> override;
    auto resize(Size) -> Result<void> override;

private:
    FaultControls m_faults;
    SharedMemory m_memory;
    Random m_random;
    std::filesystem::path m_path;
    Index m_cursor {};
    int m_permissions {};
    Mode m_mode {};
    bool m_is_append {};
    bool m_is_open {};
};

class MockDirectory: public IDirectory {
public:
    ~MockDirectory() override = default;

    explicit MockDirectory(const std::string &path):
          m_fake {path}
    {
        delegate_to_fake();
    }

    [[nodiscard]] auto is_open() const -> bool override
    {
        return m_fake.is_open();
    }

    [[nodiscard]] auto path() const -> std::string override
    {
        return m_fake.path();
    }

    [[nodiscard]] auto name() const -> std::string override
    {
        return m_fake.name();
    }

    MOCK_METHOD(Result<std::vector<std::string>>, children, (), (const, override));
    MOCK_METHOD(Result<std::unique_ptr<IFile>>, open_file, (const std::string&, Mode, int), (override));
    MOCK_METHOD(Result<void>, remove, (), (override));
    MOCK_METHOD(Result<void>, sync, (), (override));
    MOCK_METHOD(Result<void>, close, (), (override));
    MOCK_METHOD(Result<bool>, exists, (const std::string&), (const, override));

    auto delegate_to_fake() -> void
    {
        ON_CALL(*this, children).WillByDefault([this] {
            return m_fake.children();
        });
        ON_CALL(*this, open_file).WillByDefault([this](const std::string &name, Mode mode, int permissions) {
            return open_and_register_mock_file(name, mode, permissions);
        });
        ON_CALL(*this, remove).WillByDefault([this] {
            return m_fake.remove();
        });
        ON_CALL(*this, sync).WillByDefault([this] {
            return m_fake.sync();
        });
        ON_CALL(*this, close).WillByDefault([this] {
            return m_fake.close();
        });
        ON_CALL(*this, exists).WillByDefault([this](const std::string &name) {
            return m_fake.exists(name);
        });
    }

    [[nodiscard]] auto fake() -> FakeDirectory&
    {
        return m_fake;
    }

    [[nodiscard]] auto get_mock_file(const std::string &name, Mode mode) -> MockFile*
    {
        auto itr = m_files.find(mock_name(name, mode));
        CCO_EXPECT_NE(itr, end(m_files));
        return itr->second;
    }

private:
    [[nodiscard]] auto mock_name(const std::string &name, Mode mode) -> std::string
    {
        return name + std::to_string(static_cast<unsigned>(mode));
    }
    [[nodiscard]] auto open_and_register_mock_file(const std::string&, Mode, int) -> std::unique_ptr<IFile>;

    auto unregister_mock_file(const std::string &name)
    {
        [[maybe_unused]] const auto num_erased = m_files.erase(name);
        CCO_EXPECT_EQ(num_erased, 1);
    }

    std::unordered_map<std::string, MockFile*> m_files;
    FakeDirectory m_fake;
};

class MockFile: public IFile {
public:
    ~MockFile() override = default;

    explicit MockFile(std::unique_ptr<IFile> file):
          m_file {std::move(file)} {}

    [[nodiscard]] auto is_open() const -> bool override
    {
        return m_file->is_open();
    }

    [[nodiscard]] auto mode() const -> Mode override
    {
        return m_file->mode();
    }

    [[nodiscard]] auto permissions() const -> int override
    {
        return m_file->permissions();
    }

    [[nodiscard]] auto path() const -> std::string override
    {
        return m_file->path();
    }

    [[nodiscard]] auto name() const -> std::string override
    {
        return m_file->name();
    }

    [[nodiscard]] auto file() const -> int override
    {
        return m_file->file();
    }

    MOCK_METHOD(Result<Size>, size, (), (const, override));
    MOCK_METHOD(Result<void>, open, (const std::string&, Mode, int), (override));
    MOCK_METHOD(Result<void>, close, (), (override));
    MOCK_METHOD(Result<void>, rename, (const std::string&), (override));
    MOCK_METHOD(Result<void>, remove, (), (override));
    MOCK_METHOD(Result<Index>, seek, (long, Seek), (override));
    MOCK_METHOD(Result<Size>, read, (Bytes), (override));
    MOCK_METHOD(Result<Size>, read, (Bytes, Index), (override));
    MOCK_METHOD(Result<Size>, write, (BytesView), (override));
    MOCK_METHOD(Result<Size>, write, (BytesView, Index), (override));
    MOCK_METHOD(Result<void>, sync, (), (override));
    MOCK_METHOD(Result<void>, resize, (Size), (override));

    auto delegate_to_fake() -> void
    {
        using testing::_;

        ON_CALL(*this, size).WillByDefault([this] {
            return m_file->size();
        });
        ON_CALL(*this, open).WillByDefault([this](const std::string &name, Mode mode, int permissions) {
            return m_file->open(name, mode, permissions);
        });
        ON_CALL(*this, close).WillByDefault([this] {
            return m_file->close();
        });
        ON_CALL(*this, rename).WillByDefault([this](const std::string &name) {
            return m_file->rename(name);
        });
        ON_CALL(*this, remove).WillByDefault([this] {
            return m_file->remove();
        });
        ON_CALL(*this, seek).WillByDefault([this](long offset, Seek whence) {
            return m_file->seek(offset, whence);
        });
        ON_CALL(*this, read(_)).WillByDefault([this](Bytes out) {
            return m_file->read(out);
        });
        ON_CALL(*this, read(_, _)).WillByDefault([this](Bytes out, Index offset) {
            return m_file->read(out, offset);
        });
        ON_CALL(*this, write(_)).WillByDefault([this](BytesView in) {
            return m_file->write(in);
        });
        ON_CALL(*this, write(_, _)).WillByDefault([this](BytesView in, Index offset) {
            return m_file->write(in, offset);
        });
        ON_CALL(*this, sync).WillByDefault([this] {
            return m_file->sync();
        });
        ON_CALL(*this, resize).WillByDefault([this](Size size) {
            return m_file->resize(size);
        });
    }

    auto fake() -> FakeFile&
    {
        return *dynamic_cast<FakeFile*>(m_file.get());
    }

    auto fake() const -> const FakeFile&
    {
        return *dynamic_cast<const FakeFile*>(m_file.get());
    }

private:
    std::unique_ptr<IFile> m_file;
};

} // cco

#endif // CCO_TEST_TOOLS_FAKES_H

#ifndef CALICO_TEST_TOOLS_FAKES_H
#define CALICO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "db/database_impl.h"
#include "page/file_header.h"
#include "random.h"
#include "storage/interface.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"
#include "pool/buffer_pool.h"

namespace calico {

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

    FaultControls()
        : m_controls {std::make_shared<Controls>()} {}

    explicit FaultControls(std::shared_ptr<Controls> controls)
        : m_controls {std::move(controls)} {}

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
        CALICO_EXPECT_LE(rate, 100);
        m_controls->read_fault_rate = rate;
    }

    auto set_write_fault_rate(unsigned rate)
    {
        CALICO_EXPECT_LE(rate, 100);
        m_controls->write_fault_rate = rate;
    }

    auto set_read_fault_counter(int value)
    {
        CALICO_EXPECT_GE(value, -1);
        m_controls->read_fault_counter = value;
    }

    auto set_write_fault_counter(int value)
    {
        CALICO_EXPECT_GE(value, -1);
        m_controls->write_fault_counter = value;
    }

private:
    std::shared_ptr<Controls> m_controls;
};

class Memory;

class MemoryBank: public IDirectory {
public:
    ~MemoryBank() override = default;
    explicit MemoryBank(const std::string&);
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto children() const -> std::vector<std::string> override;
    auto open_directory(const std::string&) -> std::unique_ptr<IDirectory> override;
    auto open_file(const std::string&, Mode, int) -> std::unique_ptr<IFile> override;
    auto remove() -> void override;
    auto sync() -> void override {}

    [[nodiscard]] auto noex_children() const -> Result<std::vector<std::string>> override;
    auto noex_open_directory(const std::string&) -> Result<std::unique_ptr<IDirectory>> override;
    auto noex_open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> override;
    auto noex_remove() -> Result<void> override;
    auto noex_sync() -> Result<void> override;

    auto open_memory_bank(const std::string&) -> std::unique_ptr<MemoryBank>;
    auto open_memory(const std::string&, Mode, int) -> std::unique_ptr<Memory>;

private:
    std::unordered_map<std::string, SharedMemory> m_shared;
    std::unordered_map<std::string, FaultControls> m_faults;
    std::filesystem::path m_path;
};

class Memory: public IFile {
public:
    ~Memory() override = default;

    explicit Memory(const std::string &path)
        : m_path {path} {}

    Memory(const std::string &path, SharedMemory memory, FaultControls faults)
        : m_faults {std::move(faults)},
          m_memory {std::move(memory)},
          m_path {path} {}

    [[nodiscard]] auto is_open() const -> bool override
    {
        return m_is_open;
    }

    [[nodiscard]] auto is_readable() const -> bool override
    {
        return m_is_readable;
    }

    [[nodiscard]] auto is_writable() const -> bool override
    {
        return m_is_writable;
    }

    [[nodiscard]] auto is_append() const -> bool override
    {
        return m_is_append;
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

    [[nodiscard]] auto size() const -> Size override
    {
        return memory().size();
    }

    [[nodiscard]] auto open_reader() -> std::unique_ptr<IFileReader> override;
    [[nodiscard]] auto open_writer() -> std::unique_ptr<IFileWriter> override;

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

    auto open(const std::string &path, Mode mode, int permissions) -> void override
    {
        m_is_open = true;
        m_path = path;
        m_permissions = permissions;
        m_is_readable = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_ONLY)) == static_cast<unsigned>(Mode::READ_ONLY) ||
                        (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
        m_is_writable = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::WRITE_ONLY)) == static_cast<unsigned>(Mode::WRITE_ONLY) ||
                        (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
        m_is_append = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::APPEND)) == static_cast<unsigned>(Mode::APPEND);
    }

    auto close() -> void override
    {
        m_is_open = false;
    }

    // TODO: Shared memory filename?
    auto rename(const std::string&) -> void override {}

    auto remove() -> void override
    {
        m_path.clear();
    }



    [[nodiscard]] auto noex_size() const -> Result<Size> override
    {
        return m_memory.memory().size();
    }

    [[nodiscard]] auto noex_open(const std::string &path, Mode mode, int permissions) -> Result<void> override
    {
        m_is_open = true;
        m_path = path;
        m_permissions = permissions;
        m_is_readable = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_ONLY)) == static_cast<unsigned>(Mode::READ_ONLY) ||
                        (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
        m_is_writable = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::WRITE_ONLY)) == static_cast<unsigned>(Mode::WRITE_ONLY) ||
                        (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::READ_WRITE)) == static_cast<unsigned>(Mode::READ_WRITE);
        m_is_append = (static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::APPEND)) == static_cast<unsigned>(Mode::APPEND);
        return {};
    }

    [[nodiscard]] auto noex_close() -> Result<void> override
    {
        m_is_open = false;
        return {};
    }

    // TODO: Shared memory filename?
    [[nodiscard]] auto noex_rename(const std::string&) -> Result<void> override
    {
        return {};
    }

    [[nodiscard]] auto noex_remove() -> Result<void> override
    {
        m_path.clear();
        return {};
    }

private:
    FaultControls m_faults;
    SharedMemory m_memory;
    Random m_random;
    std::filesystem::path m_path;
    Index m_cursor {};
    int m_permissions {};
    bool m_is_readable {};
    bool m_is_writable {};
    bool m_is_append {};
    bool m_is_open {};
};

class MemoryReader: public IFileReader {
public:
    ~MemoryReader() override = default;
    explicit MemoryReader(Memory&);
    auto seek(long, Seek) -> void override;
    auto read(Bytes) -> Size override;
    auto read_at(Bytes, Index) -> Size override;

    auto noex_seek(long, Seek) -> Result<Index> override;
    auto noex_read(Bytes) -> Result<Size> override;
    auto noex_read_at(Bytes, Index) -> Result<Size> override;

private:
    Memory *m_memory {};
};

class MemoryWriter: public IFileWriter {
public:
    ~MemoryWriter() override = default;
    explicit MemoryWriter(Memory&);
    auto seek(long, Seek) -> void override;
    auto write(BytesView) -> Size override;
    auto write_at(BytesView, Index) -> Size override;
    auto sync() -> void override {}
    auto resize(Size) -> void override;

    auto noex_seek(long, Seek) -> Result<Index> override;
    auto noex_write(BytesView) -> Result<Size> override;
    auto noex_write_at(BytesView, Index) -> Result<Size> override;
    auto noex_sync() -> Result<void> override;
    auto noex_resize(Size) -> Result<void> override;

private:
    Memory *m_memory {};
};

class IWALReader;
class IWALWriter;

struct WALHarness final {
    explicit WALHarness(Size);
    ~WALHarness();
    SharedMemory backing;
    std::unique_ptr<MemoryBank> bank;
    std::unique_ptr<IWALReader> reader;
    std::unique_ptr<IWALWriter> writer;
};

struct FakeFilesHarness {

    explicit FakeFilesHarness(Options);
    ~FakeFilesHarness() = default;
    FakeFilesHarness(FakeFilesHarness&&) = default;
    auto operator=(FakeFilesHarness&&) -> FakeFilesHarness& = default;

    std::unique_ptr<MemoryBank> bank;
    std::unique_ptr<Memory> tree_file;
    std::unique_ptr<Memory> wal_reader_file;
    std::unique_ptr<Memory> wal_writer_file;
    SharedMemory tree_backing;
    SharedMemory wal_backing;
    FaultControls tree_faults;
    FaultControls wal_reader_faults;
    FaultControls wal_writer_faults;
    Options options;
};

struct FakeDatabase {
    explicit FakeDatabase(Options);

    SharedMemory data_backing;
    SharedMemory wal_backing;
    FaultControls data_faults;
    FaultControls wal_faults;
    std::unique_ptr<Database::Impl> db;
};

} // calico

#endif // CALICO_TEST_TOOLS_FAKES_H

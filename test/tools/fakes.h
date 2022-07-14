#ifndef CCO_TEST_TOOLS_FAKES_H
#define CCO_TEST_TOOLS_FAKES_H

#include <filesystem>
#include "calico/calico.h"
#include "random.h"
#include "storage/interface.h"

namespace cco {

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

class Memory;

class MemoryBank: public IDirectory {
public:
    ~MemoryBank() override = default;
    explicit MemoryBank(const std::string&);
    [[nodiscard]] auto path() const -> std::string override;
    [[nodiscard]] auto name() const -> std::string override;
    [[nodiscard]] auto children() const -> Result<std::vector<std::string>> override;
    [[nodiscard]] auto open_directory(const std::string&) -> Result<std::unique_ptr<IDirectory>> override;
    [[nodiscard]] auto open_file(const std::string&, Mode, int) -> Result<std::unique_ptr<IFile>> override;
    [[nodiscard]] auto remove() -> Result<void> override;
    [[nodiscard]] auto sync() -> Result<void> override;

    [[nodiscard]] auto exists(const std::string&) const -> Result<bool> override
    {
        return true;
    }

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

    [[nodiscard]] auto size() const -> Result<Size> override
    {
        return m_memory.memory().size();
    }

    [[nodiscard]] auto open(const std::string &path, Mode mode, int permissions) -> Result<void> override
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
    auto seek(long, Seek) -> Result<Index> override;
    auto read(Bytes) -> Result<Size> override;
    auto read(Bytes, Index) -> Result<Size> override;

private:
    Memory *m_memory {};
};

class MemoryWriter: public IFileWriter {
public:
    ~MemoryWriter() override = default;
    explicit MemoryWriter(Memory&);
    auto seek(long, Seek) -> Result<Index> override;
    auto write(BytesView) -> Result<Size> override;
    auto write(BytesView, Index) -> Result<Size> override;
    auto sync() -> Result<void> override;
    auto resize(Size) -> Result<void> override;

private:
    Memory *m_memory {};
};

} // calico

#endif // CCO_TEST_TOOLS_FAKES_H

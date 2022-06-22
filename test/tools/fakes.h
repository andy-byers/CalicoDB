#ifndef CALICO_TEST_TOOLS_FAKES_H
#define CALICO_TEST_TOOLS_FAKES_H

#include <gmock/gmock.h>
#include "calico/bytes.h"
#include "calico/options.h"
#include "random.h"
#include "db/database_impl.h"
#include "file/interface.h"
#include "page/file_header.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace calico {

class SharedMemory {
public:
    SharedMemory()
        : m_memory {std::make_shared<std::string>()} {}

    auto memory() -> std::string&
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

class ReadOnlyMemory: public IReadOnlyFile {
public:
    ReadOnlyMemory() = default;

    explicit ReadOnlyMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}

    ~ReadOnlyMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_memory;
    }

    [[nodiscard]] auto size() const -> Size override
    {
        return m_memory.memory().size();
    }

    auto use_direct_io() -> void override {}

    auto sync() -> void override {}

    auto seek(long, Seek) -> Index override;
    auto read(Bytes) -> Size override;

private:
    SharedMemory m_memory;
    Index m_cursor{};
};

class WriteOnlyMemory: public IWriteOnlyFile {
public:
    WriteOnlyMemory() = default;

    explicit WriteOnlyMemory(SharedMemory memory)
        : m_memory {std::move(memory)} {}

    ~WriteOnlyMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_memory;
    }

    [[nodiscard]] auto size() const -> Size override
    {
        return m_memory.memory().size();
    }

    auto use_direct_io() -> void override {}

    auto sync() -> void override {}

    auto resize(Size size) -> void override
    {
        m_memory.memory().resize(size);
    }

    auto seek(long, Seek) -> Index override;
    auto write(BytesView) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class ReadWriteMemory: public IReadWriteFile {
public:
    ReadWriteMemory() = default;

    explicit ReadWriteMemory(SharedMemory memory)
        : m_memory {std::move(memory)} {}

    ~ReadWriteMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_memory;
    }

    [[nodiscard]] auto size() const -> Size override
    {
        return m_memory.memory().size();
    }

    auto use_direct_io() -> void override {}

    auto sync() -> void override {}

    auto resize(Size size) -> void override
    {
        m_memory.memory().resize(size);
    }

    auto seek(long, Seek) -> Index override;
    auto read(Bytes) -> Size override;
    auto write(BytesView) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class LogMemory: public ILogFile {
public:
    LogMemory() = default;

    explicit LogMemory(SharedMemory memory)
        : m_memory {std::move(memory)} {}

    ~LogMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_memory;
    }

    [[nodiscard]] auto size() const -> Size override
    {
        return m_memory.memory().size();
    }

    auto use_direct_io() -> void override {}

    auto sync() -> void override {}

    auto resize(Size size) -> void override
    {
        m_memory.memory().resize(size);
    }

    auto write(BytesView) -> Size override;

private:
    SharedMemory m_memory;
    Index m_cursor{};
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

    auto controls() -> std::shared_ptr<Controls>
    {
        return m_controls;
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

class FaultyBase {
public:
    explicit FaultyBase(Random::Seed seed)
        : m_controls {std::make_shared<FaultControls::Controls>()}
        , m_random {seed} {}

    FaultyBase(FaultControls &controls, Random::Seed seed)
        : m_controls {controls.controls()}
        , m_random {seed} {}

    auto controls() -> FaultControls
    {
        return FaultControls {m_controls};
    }

protected:
    auto maybe_throw_read_error()
    {
        // If the counter is positive, we tick down until we hit 0, at which point we throw an exception.
        // Afterward, further reads will be subject to the normal read fault rate mechanism. If the counter
        // is never set, it stays at -1 and doesn't contribute to the faults generated.
        auto &counter = m_controls->read_fault_counter;
        if (!counter || m_random.next_int(99U) < m_controls->read_fault_rate)
            throw IOError {"read"};
        // Counter should settle on -1.
        counter -= counter >= 0;
    }

    auto maybe_throw_write_error()
    {
        auto &counter = m_controls->write_fault_counter;
        if (!counter || m_random.next_int(99U) < m_controls->write_fault_rate)
            throw IOError {"write"};
        counter -= counter >= 0;
    }

private:
    std::shared_ptr<FaultControls::Controls> m_controls;
    Random m_random;
};

class FaultyReadOnlyMemory
    : public ReadOnlyMemory
    , public FaultyBase
{
public:
    explicit FaultyReadOnlyMemory(Random::Seed seed = 0)
        : FaultyBase {seed} {}

    explicit FaultyReadOnlyMemory(SharedMemory memory, Random::Seed seed = 0)
        : ReadOnlyMemory {std::move(memory)}
        , FaultyBase {seed} {}

    ~FaultyReadOnlyMemory() override = default;

    auto read(Bytes) -> Size override;
};

class FaultyWriteOnlyMemory
    : public WriteOnlyMemory
    , public FaultyBase
{
public:
    explicit FaultyWriteOnlyMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}

    explicit FaultyWriteOnlyMemory(SharedMemory memory, Random::Seed seed = 0)
        : WriteOnlyMemory{std::move(memory)}
        , FaultyBase{seed} {}

    ~FaultyWriteOnlyMemory() override = default;

    auto write(BytesView) -> Size override;
};

class FaultyReadWriteMemory
    : public ReadWriteMemory
    , public FaultyBase
{
public:
    explicit FaultyReadWriteMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}

    explicit FaultyReadWriteMemory(SharedMemory memory, Random::Seed seed = 0)
        : ReadWriteMemory{std::move(memory)}
        , FaultyBase{seed} {}

    ~FaultyReadWriteMemory() override = default;

    auto read(Bytes) -> Size override;
    auto write(BytesView) -> Size override;
};

class FaultyLogMemory
    : public LogMemory
    , public FaultyBase
{
public:
    explicit FaultyLogMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}

    explicit FaultyLogMemory(SharedMemory memory, Random::Seed seed = 0)
        : LogMemory{std::move(memory)}
        , FaultyBase{seed} {}

    ~FaultyLogMemory() override = default;

    auto write(BytesView) -> Size override;
};

class IWALReader;
class IWALWriter;

struct WALHarness final {
    explicit WALHarness(Size);
    ~WALHarness();
    SharedMemory backing;
    std::unique_ptr<IWALReader> reader;
    std::unique_ptr<IWALWriter> writer;
};

struct FakeFilesHarness {

    explicit FakeFilesHarness(Options);
    ~FakeFilesHarness() = default;
    FakeFilesHarness(FakeFilesHarness&&) = default;
    auto operator=(FakeFilesHarness&&) -> FakeFilesHarness& = default;

    std::unique_ptr<FaultyReadWriteMemory> tree_file;
    std::unique_ptr<FaultyReadOnlyMemory> wal_reader_file;
    std::unique_ptr<FaultyLogMemory> wal_writer_file;
    SharedMemory tree_backing;
    SharedMemory wal_backing;
    FaultControls tree_faults;
    FaultControls wal_reader_faults;
    FaultControls wal_writer_faults;
    Options options;
};

struct FaultyDatabase {

    static auto create(Size) -> FaultyDatabase;
    auto clone() -> FaultyDatabase;

    FaultyDatabase() = default;
    FaultyDatabase(FaultyDatabase&&) = default;
    auto operator=(FaultyDatabase&&) -> FaultyDatabase& = default;

    std::unique_ptr<Database::Impl> db;
    SharedMemory tree_backing;
    SharedMemory wal_backing;
    FaultControls tree_faults;
    FaultControls wal_reader_faults;
    FaultControls wal_writer_faults;
    Size page_size {}; // TODO: Remove when we get the 'info' construct working.
};


class MockReadOnlyMemory: public IReadOnlyFile {
public:
    MockReadOnlyMemory() = default;

    explicit MockReadOnlyMemory(SharedMemory memory)
        : m_fake {std::move(memory)} {}

    ~MockReadOnlyMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_fake.memory();
    }

    auto size() const -> Size override
    {
        return m_fake.size();
    }

    auto use_direct_io() -> void override {}

    MOCK_METHOD(void, sync, (), (override));
    MOCK_METHOD(Index, seek, (long, Seek), (override));
    MOCK_METHOD(Size, read, (Bytes), (override));

    auto delegate_to_fake() -> void
    {
        ON_CALL(*this, seek)
            .WillByDefault([this](long offset, Seek whence) {
                return m_fake.seek(offset, whence);
            });
        ON_CALL(*this, read)
            .WillByDefault([this](Bytes out) {
                return m_fake.read(out);
            });
    }

private:
    ReadOnlyMemory m_fake;
};


class MockWriteOnlyMemory: public IWriteOnlyFile {
public:
    MockWriteOnlyMemory() = default;

    explicit MockWriteOnlyMemory(SharedMemory memory)
        : m_fake {std::move(memory)} {}

    ~MockWriteOnlyMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_fake.memory();
    }

    auto size() const -> Size override
    {
        return m_fake.size();
    }

    auto use_direct_io() -> void override {}

    MOCK_METHOD(void, resize, (Size), (override));
    MOCK_METHOD(void, sync, (), (override));
    MOCK_METHOD(Index, seek, (long, Seek), (override));
    MOCK_METHOD(Size, write, (BytesView), (override));

    auto delegate_to_fake() -> void
    {
        ON_CALL(*this, resize)
            .WillByDefault([this](Size size) {
                return m_fake.resize(size);
            });
        ON_CALL(*this, seek)
            .WillByDefault([this](long offset, Seek whence) {
                return m_fake.seek(offset, whence);
            });
        ON_CALL(*this, write)
            .WillByDefault([this](BytesView in) {
                return m_fake.write(in);
            });
    }

private:
    WriteOnlyMemory m_fake;
};

class MockReadWriteMemory: public IReadWriteFile {
public:
    MockReadWriteMemory() = default;

    explicit MockReadWriteMemory(SharedMemory memory)
        : m_fake {std::move(memory)} {}

    ~MockReadWriteMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_fake.memory();
    }

    auto size() const -> Size override
    {
        return m_fake.size();
    }

    auto use_direct_io() -> void override {}

    MOCK_METHOD(void, sync, (), (override));
    MOCK_METHOD(void, resize, (Size), (override));
    MOCK_METHOD(Index, seek, (long, Seek), (override));
    MOCK_METHOD(Size, read, (Bytes), (override));
    MOCK_METHOD(Size, write, (BytesView), (override));

    auto delegate_to_fake() -> void
    {
        ON_CALL(*this, resize)
            .WillByDefault([this](Size size) {
                return m_fake.resize(size);
            });
        ON_CALL(*this, seek)
            .WillByDefault([this](long offset, Seek whence) {
                return m_fake.seek(offset, whence);
            });
        ON_CALL(*this, read)
            .WillByDefault([this](Bytes out) {
                return m_fake.read(out);
            });
        ON_CALL(*this, write)
            .WillByDefault([this](BytesView in) {
                return m_fake.write(in);
            });
    }

private:
    ReadWriteMemory m_fake;
};

class MockLogMemory: public ILogFile {
public:
    MockLogMemory() = default;

    explicit MockLogMemory(SharedMemory memory)
        : m_fake {std::move(memory)} {}

    ~MockLogMemory() override = default;

    auto memory() -> SharedMemory
    {
        return m_fake.memory();
    }

    auto size() const -> Size override
    {
        return m_fake.size();
    }

    auto use_direct_io() -> void override {}

    MOCK_METHOD(void, sync, (), (override));
    MOCK_METHOD(void, resize, (Size), (override));
    MOCK_METHOD(Size, write, (BytesView), (override));

    auto delegate_to_fake() -> void
    {
        ON_CALL(*this, resize)
            .WillByDefault([this](Size size) {
                return m_fake.resize(size);
            });
        ON_CALL(*this, write)
            .WillByDefault([this](BytesView in) {
                return m_fake.write(in);
            });
    }

private:

    LogMemory m_fake;
};

} // calico

#endif // CALICO_TEST_TOOLS_FAKES_H

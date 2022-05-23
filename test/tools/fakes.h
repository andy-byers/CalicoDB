#ifndef CUB_TEST_TOOLS_FAKES_H
#define CUB_TEST_TOOLS_FAKES_H

#include "common.h"
#include "random.h"
#include "file/interface.h"
#include "bytes.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace cub {

class SharedMemory {
public:
    SharedMemory(): m_memory{std::make_shared<std::string>()} {}
    auto memory() -> std::string& {return *m_memory;}
    [[nodiscard]] auto memory() const -> const std::string& {return *m_memory;}

private:
    std::shared_ptr<std::string> m_memory;
};

class ReadOnlyMemory: public IReadOnlyFile {
public:
    ReadOnlyMemory() = default;
    explicit ReadOnlyMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~ReadOnlyMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}
    [[nodiscard]] auto size() const -> Size override {return m_memory.memory().size();}
    auto use_direct_io() -> void override {}
    auto sync() -> void override {}
    auto seek(long, Seek) -> Index override;
    auto read(Bytes) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class WriteOnlyMemory: public IWriteOnlyFile {
public:
    WriteOnlyMemory() = default;
    explicit WriteOnlyMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~WriteOnlyMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}
    [[nodiscard]] auto size() const -> Size override {return m_memory.memory().size();}
    auto use_direct_io() -> void override {}
    auto sync() -> void override {}
    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
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
        : m_memory{std::move(memory)} {}
    ~ReadWriteMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}
    [[nodiscard]] auto size() const -> Size override {return m_memory.memory().size();}
    auto use_direct_io() -> void override {}
    auto sync() -> void override {}
    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
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
        : m_memory{std::move(memory)} {}
    ~LogMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}
    [[nodiscard]] auto size() const -> Size override {return m_memory.memory().size();}
    auto use_direct_io() -> void override {}
    auto sync() -> void override {}
    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
    auto write(BytesView) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};











class FaultControls {
public:
    struct Controls {
        unsigned read_fault_rate{};
        unsigned write_fault_rate{};
    };

    explicit FaultControls(std::shared_ptr<Controls> controls)
        : m_controls{std::move(controls)} {}

    [[nodiscard]] auto read_fault_rate() const
    {
        return m_controls->read_fault_rate;
    }

    [[nodiscard]] auto write_fault_rate() const
    {
        return m_controls->write_fault_rate;
    }

    auto set_read_fault_rate(unsigned rate)
    {
        CUB_EXPECT_LE(rate, 100);
        m_controls->read_fault_rate = rate;
    }

    auto set_write_fault_rate(unsigned rate)
    {
        CUB_EXPECT_LE(rate, 100);
        m_controls->write_fault_rate = rate;
    }

private:
    std::shared_ptr<Controls> m_controls;
};

class FaultyBase {
public:
    FaultyBase() = default;
    explicit FaultyBase(Random::Seed seed)
        : m_controls{std::make_shared<FaultControls::Controls>()}
        , m_random{seed} {}

    auto controls() -> FaultControls
    {
        return FaultControls{m_controls};
    }

protected:
    auto maybe_throw_read_error()
    {
        if (m_random.next_int(99U) < m_controls->read_fault_rate)
            throw IOError{SystemError{"read", EIO}};
    }

    auto maybe_throw_write_error()
    {
        if (m_random.next_int(99U) < m_controls->write_fault_rate)
            throw IOError{SystemError{"write", EIO}};
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
        : FaultyBase{seed} {}
    FaultyReadOnlyMemory(SharedMemory memory, Random::Seed seed = 0)
        : ReadOnlyMemory{std::move(memory)}
        , FaultyBase{seed} {}
    ~FaultyReadOnlyMemory() override = default;

    auto read(Bytes) -> Size override;

private:
    friend class Fs; // TODO: Necessary?
};

class FaultyWriteOnlyMemory
    : public WriteOnlyMemory
    , public FaultyBase
{
public:
    explicit FaultyWriteOnlyMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}
    FaultyWriteOnlyMemory(SharedMemory memory, Random::Seed seed = 0)
        : WriteOnlyMemory{std::move(memory)}
        , FaultyBase{seed} {}
    ~FaultyWriteOnlyMemory() override = default;

    auto write(BytesView) -> Size override;

private:
    friend class Fs;
};

class FaultyReadWriteMemory
    : public ReadWriteMemory
    , public FaultyBase
{
public:
    explicit FaultyReadWriteMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}
    FaultyReadWriteMemory(SharedMemory memory, Random::Seed seed = 0)
        : ReadWriteMemory{std::move(memory)}
        , FaultyBase{seed} {}
    ~FaultyReadWriteMemory() override = default;

    auto read(Bytes) -> Size override;
    auto write(BytesView) -> Size override;

private:
    friend class Fs;
};

class FaultyLogMemory
    : public LogMemory
    , public FaultyBase
{
public:
    explicit FaultyLogMemory(Random::Seed seed = 0)
        : FaultyBase{seed} {}
    FaultyLogMemory(SharedMemory memory, Random::Seed seed = 0)
        : LogMemory{std::move(memory)}
        , FaultyBase{seed} {}
    ~FaultyLogMemory() override = default;

    auto write(BytesView) -> Size override;

private:
    friend class Fs;
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

class StubWALWriter: public IWALWriter {
public:
    ~StubWALWriter() override = default;
    [[nodiscard]] virtual auto block_size() const -> Size override {return 0;}
    [[nodiscard]] virtual auto has_pending() const -> bool override {return false;}
    [[nodiscard]] virtual auto has_committed() const -> bool override {return false;}
    virtual auto write(WALRecord) -> LSN override {return LSN {std::numeric_limits<uint32_t>::max()};}
    virtual auto truncate() -> void override {}
    virtual auto flush() -> LSN override {return LSN {std::numeric_limits<uint32_t>::max()};}
};


class StubWALReader: public IWALReader {
public:
    virtual ~StubWALReader() override = default;
    [[nodiscard]] virtual auto record() const -> std::optional<WALRecord> override {return std::nullopt;}
    virtual auto increment() -> bool override {return false;}
    virtual auto decrement() -> bool override {return false;}
    virtual auto reset() -> void override {}
};

} // db

#endif // CUB_TEST_TOOLS_FAKES_H

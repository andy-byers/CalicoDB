#ifndef CUB_FAKES_H
#define CUB_FAKES_H

#include "common.h"
#include "storage/interface.h"
#include "utils/slice.h"
#include "random.h"

namespace cub {

class SharedMemory {
public:
    SharedMemory(): m_memory{std::make_shared<std::string>()} {}
    auto memory() -> std::string& {return *m_memory;}
    [[nodiscard]] auto memory() const -> const std::string& {return *m_memory;}

private:
    std::shared_ptr<std::string> m_memory;
};

class ReadOnlyMemory: public ReadOnlyStorage {
public:
    ReadOnlyMemory() = default;
    explicit ReadOnlyMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~ReadOnlyMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}

    auto seek(long, Seek) -> Index override;
    auto read(MutBytes) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class WriteOnlyMemory: public WriteOnlyStorage {
public:
    WriteOnlyMemory() = default;
    explicit WriteOnlyMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~WriteOnlyMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}

    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
    auto seek(long, Seek) -> Index override;
    auto write(RefBytes) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class ReadWriteMemory: public ReadWriteStorage {
public:
    ReadWriteMemory() = default;
    explicit ReadWriteMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~ReadWriteMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}

    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
    auto seek(long, Seek) -> Index override;
    auto read(MutBytes) -> Size override;
    auto write(RefBytes) -> Size override;

private:
    friend class Fs;
    SharedMemory m_memory;
    Index m_cursor{};
};

class LogMemory: public LogStorage {
public:
    LogMemory() = default;
    explicit LogMemory(SharedMemory memory)
        : m_memory{std::move(memory)} {}
    ~LogMemory() override = default;
    auto memory() -> SharedMemory {return m_memory;}

    auto resize(Size size) -> void override {m_memory.memory().resize(size);}
    auto write(RefBytes) -> Size override;

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

    auto read(MutBytes) -> Size override;

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

    auto write(RefBytes) -> Size override;

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

    auto read(MutBytes) -> Size override;
    auto write(RefBytes) -> Size override;

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

    auto write(RefBytes) -> Size override;

private:
    friend class Fs;
};

} // cub

#endif // CUB_FAKES_H

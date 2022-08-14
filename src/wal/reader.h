/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "calico/wal.h"
#include "helpers.h"

namespace calico {

class Storage;

using RedoCallback = std::function<Status(RedoDescriptor)>;

using UndoCallback = std::function<Status(UndoDescriptor)>;

class BasicWalReader {
public:
    explicit BasicWalReader(Size block_size)
        : m_forward_reader {block_size},
          m_random_reader {block_size}
    {}

    [[nodiscard]]
    auto open(Storage &store, const std::string &wal_dir, SegmentId id) -> Status
    {
        RandomReader *file {};
        const auto path = fmt::format("{}/{}", wal_dir, id.to_name());
        auto s = store.open_random_reader(path, &file);
        if (!s.is_ok()) return s;

        m_segment_id = id;
        m_positions.clear();
        return m_forward_reader.attach(file);
    }

    [[nodiscard]]
    auto close() -> Status
    {
        // We only use one file pointer between the two reader objects.
        if (m_forward_reader.is_attached()) {
            delete m_forward_reader.detach();
        } else if (m_random_reader.is_attached()) {
            delete m_random_reader.detach();
        }
        m_positions.clear();
        return Status::ok();
    }

    [[nodiscard]]
    auto redo_all(const RedoCallback &redo) -> Status
    {
        if (m_random_reader.is_attached()) {
            auto s = m_forward_reader.attach(m_random_reader.detach());
            if (!s.is_ok()) return s;
        }
        CALICO_EXPECT_TRUE(m_forward_reader.is_attached());
        for (; ; ) {

        }
        return Status::ok();
    }

    [[nodiscard]]
    auto undo_last(const UndoCallback &undo) -> Status
    {
        for (; ; ) {

        }
        return Status::ok();
    }

private:
    SegmentId m_segment_id;
    std::vector<LogPosition> m_positions;
    SequentialLogReader m_forward_reader;
    RandomLogReader m_random_reader;
    std::string m_payload; // TODO
};

class BasicWalWriter {
public:
    explicit BasicWalWriter(LogScratchManager *manager, Size block_size)
        : m_writer {block_size},
          m_manager {manager}
    {}

    [[nodiscard]]
    auto open(Storage &store, const std::string &wal_dir, SegmentId id) -> Status
    {
        AppendWriter *file {};
        const auto path = fmt::format("{}/{}", wal_dir, id.to_name());
        auto s = store.open_append_writer(path, &file);
        if (!s.is_ok()) return s;

        m_segment_id = id;
        return m_writer.attach(file);
    }

    [[nodiscard]]
    auto close() -> Status
    {
        if (m_writer.is_attached())
            delete m_writer.detach();
        return Status::ok();
    }

    auto write(BasicWalPayloadType type, ManualScratch buffer) -> void
    {
        std::lock_guard lock {m_mutex};
        m_events.push({type, buffer});
        m_cond.notify_one();
    }

private:
    struct Event {
        BasicWalPayloadType type {};
        ManualScratch buffer;
    };

    mutable std::mutex m_mutex;
    std::condition_variable m_cond;
    std::queue<Event> m_events;
    Status m_status {Status::ok()};

    SegmentId m_segment_id;
    AppendLogWriter m_writer;
    LogScratchManager *m_manager {};
};


//class LogReader {
//public:
//    ~LogReader() override = default;
//    [[nodiscard]] static auto create(const WALParameters &) -> Result<std::unique_ptr<IWALReader>>;
//    [[nodiscard]] auto is_open() -> bool override;
//    [[nodiscard]] auto is_empty() -> Result<bool> override;
//    [[nodiscard]] auto open(std::unique_ptr<IFile>) -> Result<void> override;
//
//    // NOTE: We can always call read({0, 0}) to get the first record in the segment, even if we don't know the other positions yet.
//    [[nodiscard]] auto read(Position &) -> Result<WALRecord> override;
//    [[nodiscard]] auto close() -> Result<void> override;
//    auto reset() -> void override;
//
//private:
//    explicit WALReader(const WALParameters &);
//    [[nodiscard]] auto read_block(Size) -> Result<bool>;
//    [[nodiscard]] auto read_record(Size) -> Result<WALRecord>;
//
//    std::string m_block;
//    std::string m_scratch[2];
//    std::unique_ptr<IFile> m_file;
//    Size m_block_id {};
//    bool m_has_block {};
//};








//class IFile;
//
//class WALReader : public IWALReader {
//public:
//    ~WALReader() override = default;
//    [[nodiscard]] static auto create(const WALParameters &) -> Result<std::unique_ptr<IWALReader>>;
//    [[nodiscard]] auto is_open() -> bool override;
//    [[nodiscard]] auto is_empty() -> Result<bool> override;
//    [[nodiscard]] auto open(std::unique_ptr<IFile>) -> Result<void> override;
//
//    // NOTE: We can always call read({0, 0}) to get the first record in the segment, even if we don't know the other positions yet.
//    [[nodiscard]] auto read(Position &) -> Result<WALRecord> override;
//    [[nodiscard]] auto close() -> Result<void> override;
//    auto reset() -> void override;
//
//private:
//    explicit WALReader(const WALParameters &);
//    [[nodiscard]] auto read_block(Size) -> Result<bool>;
//    [[nodiscard]] auto read_record(Size) -> Result<WALRecord>;
//
//    std::string m_block;
//    std::string m_scratch[2];
//    std::unique_ptr<IFile> m_file;
//    Size m_block_id {};
//    bool m_has_block {};
//};
//
//class WALExplorer final {
//public:
//    using Position = IWALReader::Position;
//
//    struct Discovery {
//        WALRecord record;
//        Position position;
//    };
//
//    ~WALExplorer() = default;
//    explicit WALExplorer(IWALReader &);
//    [[nodiscard]] auto read_next() -> Result<Discovery>;
//    auto reset() -> void;
//
//private:
//    Position m_position;
//    IWALReader *m_reader {};
//};
//
} // namespace cco

#endif // CALICO_WAL_READER_H
///**
//*
//* References
//*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
//*/
//
//#ifndef CALICO_WAL_WAL_READER_H
//#define CALICO_WAL_WAL_READER_H
//
//#include "calico/wal.h"
//#include <memory>
//#include <spdlog/logger.h>
//#include <stack>
//
//namespace cco {
//
//class Storage;
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
//} // namespace cco
//
//#endif // CALICO_WAL_WAL_READER_H
//#ifndef CALICO_WAL_WAL_WRITER_H
//#define CALICO_WAL_WAL_WRITER_H
//
//#include "calico/wal.h"
//#include "page/update.h"
//#include "utils/identifier.h"
//#include <memory>
//#include <spdlog/logger.h>
//
//namespace cco {
//
//class Storage;
//class IFile;
//class WALRecord;
//
///**
// * A component that appends records to the WAL file.
// */
//class WALWriter {
//public:
//    ~WALWriter() = default;
//    [[nodiscard]] static auto create(const WALParameters &) -> Result<std::unique_ptr<IWALWriter>>;
//    [[nodiscard]] auto needs_segmentation() -> bool ;
//    [[nodiscard]] auto is_open() -> bool ;
//    [[nodiscard]] auto open(std::unique_ptr<IFile>) -> Status ;
//    [[nodiscard]] auto close() -> Status ;
//    [[nodiscard]] auto append(WALRecord) -> Result<Position> ;
//    [[nodiscard]] auto truncate() -> Status ;
//    [[nodiscard]] auto flush() -> Status ;
//
//    auto set_flushed_lsn(SeqNum flushed_lsn) -> void
//    {
//        m_flushed_lsn = flushed_lsn;
//        m_last_lsn = flushed_lsn;
//    }
//
//    [[nodiscard]] auto flushed_lsn() const -> SeqNum
//    {
//        return m_flushed_lsn;
//    }
//
//    [[nodiscard]] auto last_lsn() const -> SeqNum
//    {
//        return m_last_lsn;
//    }
//
//    [[nodiscard]] auto has_pending() const -> bool
//    {
//        return m_position.offset > 0;
//    }
//
//    [[nodiscard]] auto has_committed() const -> bool
//    {
//        return m_position.block_id > 0;
//    }
//
//private:
//    explicit WALWriter(const WALParameters &);
//
//    std::unique_ptr<IFile> m_file;
//    std::string m_tail;
//    Position m_position {};
//    SeqNum m_flushed_lsn;
//    SeqNum m_last_lsn;
//};
//
//} // namespace cco
//
//#endif // CALICO_WAL_WAL_WRITER_H
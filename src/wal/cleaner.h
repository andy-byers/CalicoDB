//#ifndef CALICO_WAL_CLEANER_H
//#define CALICO_WAL_CLEANER_H
//
//#include "helpers.h"
//#include "reader.h"
//#include "utils/worker.h"
//#include <thread>
//
//namespace calico {
//
//class BasicWalReader;
//
///*
// * WAL component that handles cleanup of obsolete segment files in the background.
// */
//class BasicWalCleaner {
//public:
//    BasicWalCleaner(Storage &store, std::string prefix, WalCollection &collection, BasicWalReader &reader)
//        : m_worker {[this](const auto &event) {
//              return on_event(event);
//          }, [this](const auto &status) {
//            return on_cleanup(status);
//          }},
//          m_prefix {std::move(prefix)},
//          m_store {&store},
//          m_reader {&reader},
//          m_collection {&collection}
//    {}
//
//    [[nodiscard]]
//    auto status() const -> Status
//    {
//        return m_worker.status();
//    }
//
//    auto dispatch(SequenceId lsn) -> void
//    {
//        m_worker.dispatch(lsn);
//    }
//
//    [[nodiscard]]
//    auto destroy() && -> Status
//    {
//        return std::move(m_worker).destroy();
//    }
//
//private:
//    auto on_event(const SequenceId &limit) -> Status
//    {
//        auto id = m_collection->id_after(SegmentId::null());
//        SequenceId first_lsn;
//        SegmentId previous;
//
//        while (!id.is_null()) {
//            auto s = m_reader->open(id);
//            if (!s.is_ok()) return s;
//
//            s = m_reader->read_first_lsn(first_lsn);
//            if (!s.is_ok()) return s;
//
//            if (first_lsn > limit)
//                break;
//
//            if (!previous.is_null()) {
//                s = m_store->remove_file(m_prefix + previous.to_name());
//                if (!s.is_ok()) return s;
//                m_collection->remove_before(id);
//            }
//            previous = id;
//            id = m_collection->id_after(id);
//        }
//        return Status::ok();
//    }
//
//    auto on_cleanup(const Status &s) -> Status
//    {
//        return s;
//    }
//
//    Worker<SequenceId> m_worker;
//    std::string m_prefix;
//    Storage *m_store {};
//    BasicWalReader *m_reader {};
//    WalCollection *m_collection {};
//};
//
//} // namespace calico
//
//#endif // CALICO_WAL_CLEANER_H

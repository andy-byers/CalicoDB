//#ifndef CALICO_WAL_CLEANER_H
//#define CALICO_WAL_CLEANER_H
//
//#include "helpers.h"
//#include <thread>
//
//namespace calico {
//
//class BasicWalReader;
//
//
//
///*
// * WAL component that handles cleanup of obsolete segment files in the background.
// */
//class BasicWalCleaner {
//public:
//    BasicWalCleaner(Storage &store, const std::string &prefix, WalCollection &collection, BasicWalReader &reader, std::atomic<SequenceId> &pager_lsn)
//        : m_prefix {prefix},
//          m_pager_lsn {&pager_lsn},
//          m_store {&store},
//          m_reader {&reader},
//          m_collection {&collection}
//    {}
//
//private:
//    std::string m_prefix;
//    std::atomic<SequenceId> *m_pager_lsn {};
//    Storage *m_store {};
//    BasicWalReader *m_reader {};
//    WalCollection *m_collection {};
//    std::thread m_thread;
//};
//
//} // namespace calico
//
//#endif // CALICO_WAL_CLEANER_H

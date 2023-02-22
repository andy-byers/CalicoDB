#ifndef CALICO_WAL_CLEANER_H
#define CALICO_WAL_CLEANER_H

#include "helpers.h"

namespace Calico {

class WalCleanup {
public:
    struct Parameters {
        Slice prefix;
        Storage *storage {};
        ErrorBuffer *error {};
        WalSet *set {};
    };

    explicit WalCleanup(const Parameters &param)
        : m_prefix {param.prefix.to_string()},
          m_storage {param.storage},
          m_error {param.error},
          m_set {param.set}
    {
        CALICO_EXPECT_FALSE(m_prefix.empty());
        CALICO_EXPECT_NE(m_storage, nullptr);
        CALICO_EXPECT_NE(m_error, nullptr);
        CALICO_EXPECT_NE(m_set, nullptr);
    }

    auto cleanup(Lsn limit) -> void;

private:
    std::string m_prefix;
    Storage *m_storage {};
    ErrorBuffer *m_error {};
    WalSet *m_set {};
};

} // namespace Calico

#endif // CALICO_WAL_CLEANER_H

#ifndef CCO_DB_BATCH_INTERNAL_H
#define CCO_DB_BATCH_INTERNAL_H

#include "calico/batch.h"

namespace cco {

class BatchInternal {
public:
    enum class EntryType {
        INSERT = 1,
        ERASE = 2,
    };

    struct Entry {
        EntryType type {};
        BytesView key;
        BytesView value;
    };

    static auto entry_count(const Batch &) -> Size;
    static auto read_entry(const Batch &, Index) -> Entry;
    static auto push_entry(Batch &, Entry) -> void;
};

} // namespace cco

#endif // CCO_DB_BATCH_INTERNAL_H

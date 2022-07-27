#ifndef CCO_BATCH_BATCH_H
#define CCO_BATCH_BATCH_H

#include <vector>
#include "database.h"
#include "status.h"

namespace cco {

class Cursor;

class Batch final {
public:
    Batch() = default;
    ~Batch() = default;
    auto insert(BytesView, BytesView) -> void;
    auto insert(const std::string&, const std::string&) -> void;
    auto insert(const Record&) -> void;
    auto erase(BytesView) -> void;
    auto erase(const std::string&) -> void;
    auto append(const Batch&) -> void;

private:
    friend class BatchInternal;

    std::vector<std::string> m_data;
};

} // cco

#endif // CCO_BATCH_BATCH_H

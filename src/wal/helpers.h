#ifndef CALICO_WAL_HELPERS_H
#define CALICO_WAL_HELPERS_H

#include "calico/storage.h"
#include "pager/delta.h"
#include "record.h"
#include "utils/expected.hpp"
#include "utils/logging.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include <map>

namespace Calico {

[[nodiscard]]
inline constexpr auto wal_block_size(Size page_size) -> Size
{
    return std::min(MAXIMUM_PAGE_SIZE, page_size * WAL_BLOCK_SCALE);
}

[[nodiscard]]
inline constexpr auto wal_scratch_size(Size page_size) -> Size
{
    const Size DELTA_PAYLOAD_HEADER_SIZE {11};
    return page_size + DELTA_PAYLOAD_HEADER_SIZE + sizeof(PageDelta);
}

} // namespace Calico

#endif // CALICO_WAL_HELPERS_H

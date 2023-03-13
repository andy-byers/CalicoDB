// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#ifndef CALICODB_WAL_READER_H
#define CALICODB_WAL_READER_H

#include "wal_record.h"

namespace calicodb
{

/* Construct for iterating over a WAL segment.
 *
 */
class WalReader
{
    Span m_tail;
    Reader *m_file {};
    std::size_t m_offset {};
    std::size_t m_block {};

public:
    explicit WalReader(Reader &file, Span tail);
    [[nodiscard]] auto read(Span &payload) -> Status;
};

} // namespace calicodb

#endif // CALICODB_WAL_READER_H
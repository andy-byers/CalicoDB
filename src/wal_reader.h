// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_READER_H
#define CALICODB_WAL_READER_H

#include "wal_record.h"

namespace calicodb
{

// Construct for iterating over a WAL segment.
class WalReader
{
    std::string *m_tail = nullptr;
    Reader *m_file = nullptr;
    std::size_t m_offset = 0;
    std::size_t m_block = 0;

public:
    explicit WalReader(Reader &file, std::string &tail);
    [[nodiscard]] auto read(std::string &out) -> Status;
};

} // namespace calicodb

#endif // CALICODB_WAL_READER_H
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TABLE_H
#define CALICODB_TABLE_H

#include <string>

namespace calicodb
{

// Persistent ordered mapping from keys to values within a CalicoDB database.
//
// Keys are unique within each table.
class Table
{
public:
    virtual ~Table();

    // Get the name used to identify this table.
    [[nodiscard]] virtual auto name() const -> const std::string & = 0;
};

} // namespace calicodb

#endif // CALICODB_TABLE_H

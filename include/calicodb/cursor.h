// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CURSOR_H
#define CALICODB_CURSOR_H

#include "slice.h"
#include "status.h"

namespace calicodb
{

// Cursor for iterating over the records in a table.
//
// Cursors must be obtained through the DB object. It should be noted that a freshly-
// allocated cursor is not considered valid until one of its seek*() methods is called.
class Cursor
{
public:
    explicit Cursor() = default;
    virtual ~Cursor() = default;

    // Check if the cursor is valid, that is, positioned on a record.
    //
    // This method must return true before key(), value(), next(), or previous() is
    // called. Otherwise, the result is unspecified.
    [[nodiscard]] virtual auto is_valid() const -> bool = 0;

    // Check the status associated with this cursor.
    //
    // If is_valid() returns true, this method will always return an OK status.
    // Otherwise, the returned status will indicate the reason why the cursor is
    // invalid. If the status is "not found", then the cursor is out of bounds.
    [[nodiscard]] virtual auto status() const -> Status = 0;

    // Get the current record key.
    //
    // Expects a valid cursor.
    [[nodiscard]] virtual auto key() const -> Slice = 0;

    // Get the current record value.
    //
    // Expects a valid cursor.
    [[nodiscard]] virtual auto value() const -> Slice = 0;

    // Move the cursor to the record with a key that is greater than or equal to
    // the given key.
    //
    // Invalidates the cursor if a read fails or the key is out of range.
    virtual auto seek(const Slice &key) -> void = 0;

    // Move the cursor to the record with the lowest key in the database, given
    // the database key ordering.
    //
    // Invalidates the cursor if a read fails or the database is empty.
    virtual auto seek_first() -> void = 0;

    // Move the cursor to the record with the highest key in the database, given
    // the database key ordering.
    //
    // Invalidates the cursor if a read fails or the database is empty.
    virtual auto seek_last() -> void = 0;

    // Move the cursor to the next record.
    //
    // The cursor is invalidated if it was at the same position as a cursor that
    // had seek_last() called on it. Expects a valid cursor.
    virtual auto next() -> void = 0;

    // Move the cursor to the previous record.
    //
    // The cursor is invalidated if it was at the same position as a cursor that
    // had seek_first() called on it. Expects a valid cursor.
    virtual auto previous() -> void = 0;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_H

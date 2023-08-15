// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CURSOR_H
#define CALICODB_CURSOR_H

#include "slice.h"
#include "status.h"

namespace calicodb
{

// Cursor for iterating over the records in a bucket
// Cursors must be obtained by passing an open Bucket to Tx::new_cursor(). It should be
// noted that a freshly-allocated cursor is not considered valid (is_valid() returns
// false) until one of its seek*() methods is called.
class Cursor
{
public:
    explicit Cursor();
    virtual ~Cursor();

    Cursor(Cursor &) = delete;
    void operator=(Cursor &) = delete;

    // Return an opaque handle representing the cursor
    // NOTE: Classes that extend Cursor must keep track of the Cursor * returned by
    //       Tx::*_bucket() and return its address from this method.
    [[nodiscard]] virtual auto handle() -> void * = 0;

    // Return true if the cursor is valid (positioned on a record), false otherwise
    // This method must return true before any of the methods key(), value(),
    // next(), or previous() are called. Those calls will result in unspecified
    // behavior if the cursor is not valid.
    [[nodiscard]] virtual auto is_valid() const -> bool = 0;

    // Return the status associated with this cursor
    // If is_valid() returns true, this method will always return an OK status.
    // Otherwise, the returned status will indicate the reason why the cursor is
    // invalid. If the status is OK, then the cursor is out of bounds but otherwise
    // valid. Cursors in this state can call, one of the seek*() to put themselves
    // back on a valid record.
    virtual auto status() const -> Status = 0;

    // Get the current record key
    // REQUIRES: is_valid()
    [[nodiscard]] virtual auto key() const -> Slice = 0;

    // Get the current record value
    // REQUIRES: is_valid()
    [[nodiscard]] virtual auto value() const -> Slice = 0;

    virtual auto find(const Slice &key) -> void = 0;

    // Move the cursor to the record with a key that is greater than or equal to
    // the given key
    // Invalidates the cursor if a read fails or the key is out of range.
    virtual auto seek(const Slice &key) -> void = 0;

    // Move the cursor to the record with the lowest key in the database, given
    // the database key ordering
    // Invalidates the cursor if a read fails or the database is empty.
    virtual auto seek_first() -> void = 0;

    // Move the cursor to the record with the highest key in the database, given
    // the database key ordering
    // Invalidates the cursor if a read fails or the database is empty.
    virtual auto seek_last() -> void = 0;

    // Move the cursor to the next record
    // REQUIRES: is_valid()
    // The cursor is invalidated if it was on the last record, i.e. at the same
    // position as a cursor that just had seek_last() called on it.
    virtual auto next() -> void = 0;

    // Move the cursor to the previous record
    // REQUIRES: is_valid()
    // The cursor is invalidated if it was on the first record, i.e. at the same
    // position as a cursor that just had seek_first() called on it.
    virtual auto previous() -> void = 0;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_H

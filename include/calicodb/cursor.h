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
// Cursors are obtained by calling Bucket::new_cursor(). It should be noted that a
// freshly-allocated cursor is not considered valid (is_valid() returns false) until
// find() or one of the seek*() methods returns an OK status.
// Cursors enforce certain guarantees to make working with them easier. If a cursor
// returns true for is_valid(), then the slices returned by key() and value() will
// not be invalidated until the cursor is moved, even if the bucket is modified by a
// different cursor. The bucket that a cursor is open on can be closed before the
// cursor itself. Such a cursor will be invalidated, however, calling seek*() or
// find() is not allowed. The only thing one can do with a stranded cursor is call
// delete on it.
class Cursor
{
public:
    explicit Cursor();
    virtual ~Cursor();

    Cursor(Cursor &) = delete;
    void operator=(Cursor &) = delete;

    // Return an opaque handle representing the cursor
    // NOTE: Classes that extend Cursor must return the pointer returned by the handle()
    //       method of the original cursor.
    [[nodiscard]] virtual auto handle() -> void * = 0;

    // Return true if the cursor is valid (positioned on a record), false otherwise
    // This method must return true before any of the methods key(), value(),
    // next(), or previous() are called. Those calls will result in unspecified
    // behavior if the cursor is not valid.
    [[nodiscard]] virtual auto is_valid() const -> bool = 0;

    // Return true if the cursor is positioned on a bucket record, false otherwise
    // Cursors positioned on a bucket will return an empty slice from value(). The
    // referred-to sub bucket can be opened by calling open_bucket(c.key(), ...),
    // where c is the current cursor.
    [[nodiscard]] virtual auto is_bucket() const -> bool = 0;

    // Return the status associated with this cursor
    // If is_valid() returns true, this method will always return an OK status.
    // Otherwise, the returned status will indicate the reason why the cursor is
    // invalid. If the status is OK, then the cursor is out of bounds but otherwise
    // valid. Invalid cursors can call find() or one of the seek*() to put themselves
    // back on a valid record.
    virtual auto status() const -> Status = 0;

    // Return the current key
    // REQUIRES: is_valid()
    [[nodiscard]] virtual auto key() const -> Slice = 0;

    // Return the current value
    // REQUIRES: is_valid()
    [[nodiscard]] virtual auto value() const -> Slice = 0;

    // Move the cursor to the first record with a key that is equal to the given `key`
    // If the record is found, then c->is_valid() will return true on the cursor c,
    // otherwise, it will return false. If an error is encountered, c->status() will
    // return a non-OK status describing what happened.
    virtual auto find(const Slice &key) -> void = 0;

    // Move the cursor to the first record with a key that is greater than or equal
    // to the given `key`
    // Invalidates the cursor if a read fails or the key is out of range.
    virtual auto seek(const Slice &key) -> void = 0;

    // Move the cursor to the record with the lowest-ranked key in the bucket
    // Invalidates the cursor if a read fails or the database is empty. On success,
    // the cursor is left on the leftmost key in the bucket. Calling previous() on
    // such a cursor will cause it to be invalidated.
    virtual auto seek_first() -> void = 0;

    // Move the cursor to the record with the highest-ranked key in the bucket
    // Invalidates the cursor if a read fails or the database is empty. On success,
    // the cursor is left on the rightmost key in the bucket. Calling next() on such
    // a cursor will cause it to be invalidated.
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

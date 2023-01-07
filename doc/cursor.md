# Cursors
In Calico DB, records are accessed exclusively through cursors.
Cursors provide access to a single record at a time.
They store their position in the database, and can be used to traverse in either direction.
Since we are using a B<sup>+</sup>-tree, all records are stored in external nodes.
Thus, cursors are confined to the bottom row of the tree and move around using sibling node links.

A valid cursor is one that is positioned on an existing database record, while an invalid cursor is one that is not.
When a cursor moves out of range, it is marked invalid and cannot be restored.
Invalid cursors make no distinction between "end" (one past the last element) and the "rend" (one before the first element) positions.
While this prohibits us from achieving an ordering between any two cursors, we can still use equality comparison (with the caveat that all invalid cursors compare the same).
This means that iterating toward an invalid cursor is equivalent to iterating to the end of the database.
For example, the following snippet will iterate through the whole database.
```C++
// Here we assume that the database is nonempty and does not contain "xyz", or anything that compares 
// greater than it.
const auto bounds = db.find(calico::Slice {"xyz")};
assert(not bounds.is_valid());

// This is effectively the same as iterating while `c.is_valid()`.
for (auto c = db.first(); c != bounds; c++) {
    // Do something with c.key() and/or c.value().
}
```

Calico DB supports multiple readers.
We can use cursors to perform queries on a database from multiple threads at once.
However, we cannot write to the database from multiple threads, nor can we write while we are reading.
Calico DB does not provide any protection against this, it only guarantees that multiple concurrent cursors work properly.
If a user wants to interleave readers and writers, an external reader-writer lock can be used.


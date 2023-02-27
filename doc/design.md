# CalicoDB Design
CalicoDB is designed to be very simple to use.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree backend and a write-ahead log (WAL).
Other core modules are located in the `src` directory.
The database is generally represented on disk by a single directory.
The B<sup>+</sup>-tree containing the record store is located in a file called `data` in the main directory.
The WAL segment files can either be located in the main directory, or in a different location, depending on the `wal_prefix` initialization option.
The info log will be created in the main directory as well, unless a custom `Logger *` is passed to the database.
CalicoDB runs in a single thread.

### Storage
The storage module handles platform-specific filesystem operations and I/O.
Users can override classes in [`calico/storage.h`](../include/calico/storage.h).
Then, a pointer to the custom `Storage` object can be passed to the database during when it is opened.
See [`test/tools`](../test/tools) for an example that stores the database in memory.

### Pager
The pager module provides in-memory caching for database pages read by the `storage` module.
It is the pager's job to maintain consistency between database pages on disk and in memory, and to coordinate with the WAL.
The pager contains logic to make sure that updates hit the WAL before the database file, ensuring that we can always recover from a crash.

### Tree
The B<sup>+</sup>-tree logic can be found in the tree module.
The tree works similarly to an index B-tree in SQLite3.
It is of variable order, so splits are performed when nodes have run out of physical space.
The implementation is pretty straightforward: we basically do as little as possible to make sure that the tree ordering remains correct.
This results in a less-balanced tree, but seems to be good for write performance.

#### Fixing Overflows
As previously mentioned, tree nodes are split when they run out of room.
To keep things simple, we allow each node to overflow by a single cell.
Once a node overflows, we will work to resolve it immediately.

#### Fixing Underflows
We merge 2 nodes together when one of them has become empty.
Since a merge involves addition of the separator cell, this may not be possible if the second node is very full.
In this case, we will perform a single rotation.

### WAL
The WAL record format is similar to that of `RocksDB`.
Additionally, we have 2 WAL payload types: deltas and full images.
A full image is generated the first time a page is modified during a transaction.
A full image contain a copy of the page before anything was changed, and can be used to undo all modifications made during the transaction.
Further modifications to the page will produce deltas, which record only the changed portions of the page (just the "after" contents).
Note that full image records are always disjoint (w.r.t. the affected page IDs) within a single transaction.
This means that they can be applied to the database in any order and produce the same results.
Deltas are not disjoint, so they must be read in order.
Commits are signified by a special delta record: one that updates exactly the file header on the root page.
This record modifies the database commit LSN, which is the LSN of the most recent commit record.
On normal database shutdown, we read through the WAL and undo all uncommitted changes.
During startup, if the database has WAL segments, then we know something went wrong last time.

### Consistency
CalicoDB must enforce certain rules to maintain consistency between the WAL and the database.
First, we need to make sure that all updates are written to the WAL before affected pages are written back to the database file.
The WAL keeps track of the last LSN it flushed to disk (the `flushed_lsn`).
This value is queried by the pager to make sure that unprotected pages are never written back.
The pager keeps track of a few more variables to ensure consistency: the `page_lsn`, the `record_lsn`, the `commit_lsn`, and the `recovery_lsn`.
The `page_lsn` (per page) is the LSN of the last WAL record generated for a given page.
This is the value that is compared with the WAL's `flushed_lsn` to make sure the page is safe to write out.
The `record_lsn` (also per page) is the last `page_lsn` value that we already have on disk.
It is saved (in-memory only) when the page is first read into memory, and each time the page is written back.
Then, the lowest `record_lsn` is tracked in the pager's `recovery_lsn`.
The `recovery_lsn` represents the oldest WAL record that we still need.
It is reported back to the WAL intermittently so that obsolete segment files can be removed.
Finally, the `commit_lsn` is the LSN of the most-recent commit record (the special delta record mentioned in [WAL](#wal)).
This value is saved in the database file header and is used to determine how the logical contents of the database should look.
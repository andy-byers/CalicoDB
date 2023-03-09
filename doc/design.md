# CalicoDB Design
CalicoDB is designed to be very simple to use.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree with a write-ahead log (WAL).
The B<sup>+</sup>-tree containing the record store is located in a file with the same name as the database.
By default, the WAL segment files are named the same as the database, but with a suffix of `-wal-N`, where `N` is some integer.
The info log file is similarly named, with a suffix of `-log`, unless a custom `InfoLogger *` is passed to the database during creation.
CalicoDB runs in a single thread.

### Env
The env construct handles platform-specific filesystem operations and I/O.
Users can override classes in [`calicodb/env.h`](../include/calicodb/env.h).
Then, a pointer to the custom `Env` object can be passed to the database when it is opened.
See [`test/tools`](../test/tools) for an example that stores the database files in memory.

### Pager
The pager module provides in-memory caching for database pages read by the `Env`.
It is the pager's job to maintain consistency between database pages on disk and in memory, and to coordinate with the WAL.
The pager contains logic to make sure that updates hit the WAL before the database file, ensuring that we can always recover from a crash.
In an effort to reduce the impact of sequential scans on the cache, the pager uses a 2Q-like page replacement policy.

### Tree
CalicoDB trees are similar to index B-trees in SQLite3.
Every tree is rooted on some database page, called its root page.
The root tree is the tree that is rooted on the first database page.
It is used to store a name-to-root mapping for the other trees, if any exist.
Additional trees will be rooted on pages after the second database page, which is always a pointer map page (see [Pointer Map](#pointer-map)).
Trees are of variable order, so splits are performed when nodes (pages that are part of a tree) have run out of physical space.
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

#### Pointer map
[//]: # (TODO)

#### Overflow chains
CalicoDB supports very large keys and/or values.
When a key or value is too large to fit on a page, some of it is transferred to an overflow chain.

[//]: # (TODO)

#### Freelist
[//]: # (TODO)

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
The pager keeps track of a few more variables to ensure consistency: the `page_lsn`, the `record_lsn`, the `checkpoint_lsn`, and the `recovery_lsn`.
The `page_lsn` (per page) is the LSN of the last WAL record generated for a given page.
This is the value that is compared with the WAL's `flushed_lsn` to make sure the page is safe to write out.
The `record_lsn` (also per page) is the last `page_lsn` value that we already have on disk.
It is saved (in-memory only) when the page is first read into memory, and each time the page is written back.
Then, the lowest `record_lsn` is tracked in the pager's `recovery_lsn`.
The `recovery_lsn` represents the oldest WAL record that we still need.
It is reported back to the WAL intermittently so that obsolete segment files can be removed.
Finally, the `checkpoint_lsn` is the LSN of the most-recent commit record (the special delta record mentioned in [WAL](#wal)).
This value is saved in the database file header and is used to determine how the logical contents of the database should look.

### Notes for tables support
Here are the main things that need to change:
1. Each tree is rooted on some database page. The root tree, rooted on the first page, stores a mapping from table names to tree roots.
2. Each tree needs a tree header, which will store its commit LSN, and possibly a record count
    + Tree header is located after the page header but before the node header 
3. Vacuum needs a special case for swapping a tree root. We can also get rid of empty unopened tables in vacuum.
4. Commit and recovery need to be modified
    + There is still 1 WAL, and records must know what tree they came from
    + A commit record is the delta record that updates the tree/file header and page LSN, that never happens outside of commit
    + Recovery reads the WAL and determines the last LSN written for each table, as well as the commit LSN for each table
    + Then, it reverts all uncommitted changes in another pass
    + Like before, the WAL is removed once the database is consistent and all dirty pages have been flushed
6. When a table is closed with uncommitted changes, we will need to roll them back right away
    + We will have to update the table's commit LSN (maybe it should be called a checkpoint LSN) to match the last LSN for that table (get that from reading the WAL)
    + The updated LSN needs to be written to disk immediately, otherwise, the recovery routine could get confused if we cleaned up WAL segments later. 
It will apply delta records thinking they are needed, fail to find a commit record, then try to roll back.
It won't be able to undo all the changes if it is missing any full images from the left.
If the commit LSN is higher than any of these records, none of them will be considered during recovery, so that seems to solve the problem.
This is the one change that isn't recorded in the WAL.

**NOTE: Waiting on implementing per-table checkpoints**
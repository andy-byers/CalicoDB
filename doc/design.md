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

#### Fixing Overflows
As previously mentioned, tree nodes are split when they run out of room.
To keep things simple, each node is allowed to overflow by a single cell, and overflows are resolved immediately.
There are 3 general routines for resolving node overflows: `split_root`, `split_nonroot`, and `split_nonroot_fast`.
Note that these routines are greatly simplified compared to the similarly-named balancing routines in SQLite.
For instance, SQLite attempts to involve 3 nodes in their split to better redistribute cells, while this implementation only uses 2.

`split_nonroot` is called when a non-root node has overflowed.
First, allocate a new node, called `R`.
Let the node that is overfull be `L` and the parent `P`.
Start transferring cells from `L` to `R`, checking at each point if the overflow cell `c` will fit in `L`.
There will be a point (once we reach the "overflow index") at which there have been enough cells transferred that `c` belongs in `R`.
If this point is reached, then `c` is written to `R`, since it must fit.
This routine takes advantage of the fact that the local size of a cell is limited to roughly 1/4 of the usable space on a page.
If a maximally-sized overflow cell will not fit in `L` then it definitely will fit in `R`, since `R` was empty initially.
This is true even if `L` was 100% full before the overflow.
Note that this splitting routine is optimal when keys are sequential and decreasing, but has its worst case when keys are sequential and increasing.
When a node overflows on the rightmost position, `split_nonroot_fast` is called.
`split_nonroot_fast` allocates a new right sibling and writes the overflow cell to it.
This makes consecutive sequential writes much faster, as well as sequential writes that are at the top of the key range.

`split_root` is called when a tree root has become overfull.
It transfers the root's contents into a new node, which is set as the empty root's rightmost child.
`split_nonroot` is then called on the overfull child, which posts a separator to the empty root.
This leaves the root with a single cell and 2 children.

#### Fixing Underflows
Sibling nodes are merged together when one of them has become empty.
Since a merge involves addition of the separator cell, this may not be possible if the second node is very full.
In this case, a single rotation is performed.

#### Overflow chains
CalicoDB supports very large keys and/or values.
When a key or value is too large to fit on a page, some of it is transferred to an overflow chain.

[//]: # (TODO)

#### Freelist
[//]: # (TODO)

#### Pointer map
[//]: # (TODO)

Each cell that is moved between internal tree nodes must have its child's parent pointer updated.
If the parent pointers are embedded in the nodes, splits and merges become very expensive, since each transferred cell requires the child page to be written.
In addition to facilitating the vacuum operation, pointer maps make splits and merges much more efficient by consolidating many parent pointers on a single page.

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
The WAL keeps track of the last LSN that `write()` was called on (the `written_lsn`), and the last LSN definitely on disk (the `flushed_lsn`).
The `written_lsn` is increased when the tail buffer is flushed, and the `flushed_lsn` is increased when `fsync()` is called.
The `flushed_lsn` is always less than or equal to the `written_lsn`.
The `flushed_lsn` is queried by the pager to make sure unprotected pages are never written back.
The pager keeps track of a few more variables to ensure consistency: the per-page `page_lsn`, the per-page `record_lsn`, the `checkpoint_lsn`, and the `recovery_lsn`.
The `page_lsn` is the LSN of the last WAL record generated for a given page.
This is the value that is compared with the WAL's `flushed_lsn` to make sure the page is safe to write out.
The `record_lsn` is the last `page_lsn` value that is already on disk.
It is saved (in-memory only) when the page is first read into memory, and each time the page is written back.
The `recovery_lsn` represents the oldest WAL record that we still need.
Each time the database file is `fsync()`'d, the `recovery_lsn` is updated to match the oldest `record_lsn` for a dirty cached page.
Every change before this point must be safely on disk.
Then, the `recovery_lsn` is checked against the WAL segments to determine if any can be removed.
Finally, the `checkpoint_lsn` is the LSN of the most-recent checkpoint record (the special delta record mentioned in [WAL](#wal)).
This value is saved in the database file header and is used to determine how the logical contents of the database should look.
The database must ensure that there is always enough information in the WAL to revert to the most-recent checkpoint.

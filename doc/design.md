# CalicoDB Design
CalicoDB is designed to be very simple to use.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.
The concurrency model is based off SQLite in WAL mode.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree backed by a write-ahead log (WAL).
The record store consists of 0 or more B<sup>+</sup>-trees and is located in a file with the same name as the database.
Given that a process accessing a CalicoDB database shuts down properly, the record store is all that is left on disk.

While running, however, 2 additional files are maintained:
+ `-wal`: The `-wal` file is named like the database file, but with a suffix of `-wal`.
Updates to database pages are written here, rather than the database file.
See [WAL file](#wal-file) for details.
+ `-shm`: The `-shm` file is named like the `-wal` file, but with a suffix of `-shm` in place of `-wal`.
Greatly speeds up the process of locating specific database pages in the WAL.
See [shm file](#shm-file) for details.

### Env
The env construct handles platform-specific filesystem operations and I/O.
Users can override classes in [`calicodb/env.h`](../include/calicodb/env.h).
Then, a pointer to the custom `Env` object can be passed to the database when it is opened.
See [`fake_env.h`](../utils/fake_env.h) for an example that stores the database files in memory.

### Pager
The pager module provides in-memory caching for database pages read by the `Env`.
It is the pager's job to maintain consistency between database pages on disk and in memory, and to coordinate with the WAL.

### Tree
CalicoDB trees are similar to table B-trees in SQLite3.
Every tree is rooted on some database page, called its root page.
The tree that is rooted on the first database page is called the schema tree.
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
There will be a point (once we reach the "overflow slot") at which there have been enough cells transferred that `c` belongs in `R`.
If this point is reached, then `c` is written to `R`, since it must fit.
This routine takes advantage of the fact that the local size of a cell is limited to roughly 1/4 of the usable space on a page.
If a maximally-sized overflow cell will not fit in `L` then it definitely will fit in `R`, since `R` was empty initially.
This is true even if `L` was 100% full before the overflow.
Note that this splitting routine is optimal when keys are sequential and decreasing, but has its worst case when keys are sequential and increasing.
When a node overflows on the rightmost position, `split_nonroot_fast` is called.
`split_nonroot_fast` allocates a new right sibling and writes the overflow cell to it.
This makes resolving overflows on the rightmost position much faster, which helps sequential write performance.

#### Fixing Underflows
Sibling nodes are merged together when one of them has become empty.
Since a merge involves addition of the separator cell, this may not be possible if the second node is very full.
In this case, a single rotation is performed.

#### Overflow chains
CalicoDB supports very large keys and/or values.
When a key or value is too large to fit on a page, some of it is transferred to an overflow chain.

[//]: # (TODO)

#### Freelist
Sometimes, database pages end up becoming unused.
This happens, for example, when a record with an overflow chain is erased.
Unused database pages are managed using the freelist.
There are 2 types of freelist pages: trunks and leaves.
Freelist trunk pages form a linked list threaded through the database file.
Each trunk page contains the following information:

| Offset | Size   | Name    | Purpose                                              |
|:-------|:-------|:--------|:-----------------------------------------------------|
| 0      | 4      | NextPtr | Page ID of the next freelist trunk page              |
| 4      | 4      | LeafCnt | Number of freelist leaf page IDs stored on this page |
| 8      | PgSz-8 | Leaves  | `LeafCnt` leaf page IDs                              |

The last trunk page has its `NextPtr` field set to 0.
Freelist leaf pages contain no pertinent information.
They are not written to the WAL, nor are they stored in the pager cache.

#### Pointer map
[//]: # (TODO)

Each cell that is moved between internal tree nodes must have its child's parent pointer updated.
If the parent pointers are embedded in the nodes, splits and merges become very expensive, since each transferred cell requires the child page to be written.
In addition to facilitating the vacuum operation, pointer maps make splits and merges much more efficient by consolidating many parent pointers on a single page.

### WAL

#### WAL file
As described in [Architecture](#architecture), a database named `~/cats` will store its WAL in a file named `~/cats-wal`.
This file, hereafter called the WAL file, is opened the first time a transaction is started on the database.
The WAL file consists of a fixed-length header, followed by 0 or more WAL frames.
Each WAL frame contains a single database page, along with some metadata.

Most writes to the WAL are sequential, the exception being when a page is written out more than once within a transaction.
In that case, the most-recent version of the page will be overwritten.
This lets the number of frames added to the WAL be proportional to the number of pages modified during a given transaction.

#### shm file
Since the database file is never written during a transaction, its contents quickly become stale.
This means that pages that exist in the WAL must be read from the WAL, not the database file.
The shm file is used to store the WAL index data structure, which provides a way to quickly locate pages in the WAL.
We also use the shm file to coordinate locks on the WAL.

## Database file format
The database file consists of 0 or more fixed-size pages.
A freshly-created database is just an empty database file.
When the first bucket is created, the first 3 database pages are initialized in-memory.
The first page in the file, called the root page, contains the file header and serves as the [schema tree's][#schema] root node.
The second page is always a [pointer map](#pointer-map) page.
The third page is the root node of the tree representing the first user bucket.
As the database is modified, additional pages are created by extending the database file.

## Performance
CalicoDB runs in a single thread, and is very much I/O-bound as a result.
In order to make the library fast enough to be useful, several layers of caching and buffering are used.
First, there is the pager layer, which caches database pages in memory.
Dirty pages are written to the WAL on commit or eviction from the cache.
This lets a running transaction modify a given page multiple times before it needs to be written out.
Eventually, the WAL will need to be written back to the database file.
When this happens, each unique page in the WAL is written back to the database exactly once.
The pages are also sorted by page number, so each write during the checkpoint is sequential.

## Concurrency
Concurrency in CalicoDB is based off of WAL mode SQLite.
The language used here is similar to that of their WAL documentation, but is likely to differ in some places.

CalicoDB is designed to support both multithread and multiprocess concurrency, with some caveats.
First, methods on the `DB` and all objects produced from it are not safe to call in a multithreaded context.
Each thread must have its own `DB`, but threads in the same process can share an `Env`.
Second, there can only be a single writer at any given time.
Multiple simultaneous readers are allowed to run while there is an active writer, however.
This means that if a database is being accessed by multiple threads or processes, certain calls may return with `Status::busy()`.

Concurrency control in CalicoDB relies on a few key mechanisms described here.
First, locking is coordinated on the shm file, using the `File::shm_lock()` API.
There are 8 lock bytes available:

| 0       | 1      | 2      | 3       | 4       | 5       | 6       | 7       |
|:--------|:-------|:-------|:--------|:--------|:--------|:--------|:--------|
| `Write` | `Ckpt` | `Rcvr` | `Read0` | `Read1` | `Read2` | `Read3` | `Read4` |

Each lock byte can be locked in either `kShmReader` or `kShmWriter` mode.
As the names imply, `kShmWriter` is an exclusive lock, and `kShmReader` is a shared lock.

Once a new connection has obtained a valid copy of the WAL index header (described below), it will attempt to find a "readmark" to use.
There are 5 readmarks, each protected by one of the 5 read locks (`Read*` above).
Readmarks are used by connections to store their current "max frame" value, which is found in the local copy of the index header.
New connections may use an existing readmark by taking a read lock on the corresponding read lock byte.
This indicates to other connections what portion of the WAL is being read by readers attached to that readmark.
The first readmark always has a value of 0, indicating that readers are ignoring the WAL completely.
If a new connection finds that the WAL is empty, it will attempt to use the first readmark.


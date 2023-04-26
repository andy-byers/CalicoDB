# CalicoDB Design
CalicoDB is designed to be very simple to use.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree backed by a write-ahead log (WAL).
The B<sup>+</sup>-tree containing the record store is located in a file with the same name as the database.
Given that a process accessing a CalicoDB database shuts down properly, the tree file is all that is left on disk.

While running, however, 2 additional files are managed:
+ `-wal`: The `-wal` file is named like the database file, but with a suffix of `-wal`.
Updates to the database are written here, rather than the database file.
See [`-wal` file](#-wal-file) for details.
+ `-shm`: The `-shm` file is named like the `-wal` file, but with a suffix of `-shm` in place of `-wal`.
Greatly speeds up the process of locating specific database pages in the WAL.
See [`-shm` file](#-shm-file) for details.

CalicoDB runs in a single thread, but is designed to support both multithread and multiprocess concurrency, with some caveats.
First, methods on the `DB` and all objects produced from it are not safe to call in a multithreaded context.
Each thread must have its own `DB`, but threads in the same process can share an `Env`.
Second, there can only be a single writer at any given time.
Multiple simultaneous readers are allowed to run while there is an active writer, however.
This means that if a database is being accessed by multiple threads or processes, certain calls may return with `Status::busy()`.

### Env
The env construct handles platform-specific filesystem operations and I/O.
Users can override classes in [`calicodb/env.h`](../include/calicodb/env.h).
Then, a pointer to the custom `Env` object can be passed to the database when it is opened.
See [`test/tools`](../test/tools) for an example that stores the database files in memory.

### Pager
The pager module provides in-memory caching for database pages read by the `Env`.
It is the pager's job to maintain consistency between database pages on disk and in memory, and to coordinate with the WAL.

### Tree
CalicoDB trees are similar to slot B-trees in SQLite3.
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
[//]: # (TODO)

#### Pointer map
[//]: # (TODO)

Each cell that is moved between internal tree nodes must have its child's parent pointer updated.
If the parent pointers are embedded in the nodes, splits and merges become very expensive, since each transferred cell requires the child page to be written.
In addition to facilitating the vacuum operation, pointer maps make splits and merges much more efficient by consolidating many parent pointers on a single page.

### WAL

#### `-wal` file
As described in [Architecture](#architecture), a database named `~/cats` will store its WAL in a file named `~/cats-wal`.
This file, hereafter called the WAL file, is opened the first time a transaction is started on the database.
The WAL file consists of a fixed-length header, followed by 0 or more WAL frames.
Each WAL frame contains a single database page, along with some metadata.

Most writes to the WAL are sequential, the exception being when a page is written out more than once within a transaction.
In that case, the old version of the page will be overwritten.
This lets the number of frames added to the WAL be proportional to the number of pages modified during a given transaction.

#### `-shm` file
Since the database file is never written during a transaction, its contents quickly become stale.
Any page that exists in the WAL must be read from the WAL, not the database file.
The shm file is used to store the WAL index data structure, which provides a way to quickly locate pages in the WAL.
We also use the shm file to coordinate locks on the WAL.

### NOTES

#### TODO
1. Startup
   1. WAL startup
   2. Pager startup
2. Transactions
   1. State/mode transitions
      + Pager: kOpen -> kRead (-> kWrite -> kDirty -> kWrite) -> kOpen
      + WAL: 
3. Checkpoints

#### Startup
Readonly DB connections should fail if the DB file doesn't exist.
It's fine if the WAL doesn't exist.
If the connection is exclusive, an exclusive lock is taken on the DB file, and the WAL if it exists.
If this connection succeeds, i.e. gets exclusive locks, it must be the only connection.
If there is a WAL, then a checkpoint needs to be run.
When a normal DB connection is opened on a file named `db`, the following steps are taken:
1. First, a shared lock is taken on the DB file and the file header is read. 
This version of the header may be outdated, but that's okay. 
We really just need to make sure the file is a CalicoDB database and determine the database page size.
2. If the file is empty, then wait on an exclusive lock on the DB file.
Once we have an exclusive lock, unlink the WAL file, if it exists.
Then, write the initial file header and the node header for the root page and drop the lock.
The database is now initialized.
Notice that the WAL file is not open at this point.

#### Read-only transactions
We start a read-only transaction by taking a shared lock on the DB file.
Then, we check for a WAL.
If there isn't a WAL, then all reads will come from the DB file.
If there is a WAL, we open it along with the shm file (WAL index).
The shm file is always cleared when the first connection to it is made.
Find a read mark to use and lock it?
May be able to share a read mark with another reader.
Should be able to use (a) the current "backfill count", and (b) the current "max frame" value, and (c) the read marks to determine what pages to read from the WAL, and what pages to read from the DB.
We may have to purge the pager cache if another connection wrote since we last connected.
When the reader is finished, drop the shm lock, then drop the DB file lock.

#### Read-write transactions
Like with read-only transactions, we first take a shared lock on the DB file.
Writing to the database doesn't actually write to the DB file, just the WAL, so we never need more than a shared lock.
Check for a WAL.
If no WAL exists, create one.
Either way, lock the "writer" byte in the shm file.
I believe it's fine to connect as a writer while there are still readers.
Readers stay confined to their predetermined range anyway, so we won't get in their way.
If a reader attempts to connect while a writer is writing, it will choose a read mark that prevents it from intersecting with new content.
I suppose writers may need to mess with read marks as well, I'll look into it...

#### Checkpoints
This is really the only time the DB file is written (besides creation of a new DB file).


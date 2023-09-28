# CalicoDB Design
CalicoDB aims to be simple and portable.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.
The concurrency model is based off SQLite in WAL mode.

## Portability
The core library can be configured to use only freestanding C++ standard library headers (also requires `<cstring>` for `std::memcpy` etc.), in addition to the platform-specific headers required by the default `Env` implementation.
CalicoDB can be ported to new platforms by implementing the virtual `Env` interface in [`env.h`](../include/calicodb/env.h).

The default WAL uses shared memory for synchronization.
If a certain platform does not support shared-memory, it may be possible to implement a custom `Wal` (see [`wal.h`](../include/calicodb/wal.h)) that uses a different synchronization mechanism.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree backed by a write-ahead log (WAL).
The record store consists of 0 or more B<sup>+</sup>-trees and is located in a file with the same name as the database.
Given that a process accessing a CalicoDB database shuts down properly, the record store is all that is left on disk.

While running, however, 2 additional files are maintained:
+ `-wal`: The `-wal` file is named like the database file, but with a suffix of `-wal`.
Pages to database pages are written here, rather than the database file.
See [WAL file](#wal-file) for details.
+ `-shm`: The `-shm` file is named like the `-wal` file, but with a suffix of `-shm` in place of `-wal`.
Greatly speeds up the process of locating specific database pages in the WAL.
See [shm file](#shm-file) for details.

### Env
The `Env` construct handles platform-specific filesystem operations and I/O.
Users can override classes in [`calicodb/env.h`](../include/calicodb/env.h).
Then, a pointer to the custom `Env` object can be passed to the database when it is opened.
See [`fake_env.h`](../utils/fake_env.h) for an example that stores the database files in memory for test reproducibility.

### Pager
The pager layer uses an `Env` to provide access to the database file in fixed-size chunks, called pages.
Pages are the basic unit of I/O for a CalicoDB database, and are fixed at database creation time.

It is the pager's job to maintain consistency between database pages on disk and in memory, and to coordinate with the WAL.
In this design, the pager is never allowed to write directly to the database file.
Instead, modified pages are flushed to the WAL on commit or eviction from the cache.

### Tree
As mentioned earlier, CalicoDB uses B<sup>+</sup>-trees, hereafter called trees, to store records on disk.
Every tree is rooted on some database page, called its root page.
The tree that is rooted on the first database page is called the schema tree.
The schema tree is used to maintain a name-to-root mapping for the other trees, if any exist.
Additional trees will be rooted on pages after the second database page, which is always a pointer map page (see [Pointer Map](#pointer-map)).
Trees are of variable order, so splits are performed when nodes (pages that are part of a tree) have run out of physical space.
Merges/rotations are performed when a node has become totally empty.

#### Tree nodes
A tree node is a database page that is part of a tree.
Tree nodes can be 1 of 2 types: internal or external.
External nodes store records (key-value pairs) sorted by key.
They are arranged as a doubly-linked list, with the leftmost node containing the smallest keys.
The external nodes in a tree can be thought of as a single sorted array of records, split up over multiple pages.
This makes sequential and reverse sequential scans very fast.
We just walk the linked list of external nodes, iterating through each node in order.
Internal nodes are used to direct searches to the correct external node.
They contain only keys, called pivot keys in this document.
CalicoDB uses the suffix truncation described in [2] to reduce pivot key lengths (see [Rebalancing](#rebalancing)).

All nodes use a type of "slotted pages" layout that consists of the following 5 regions:
1. Header(s)
2. Indirection vector
3. Gap space
4. Cell content area
5. Free blocks and fragments

The headers are always stored at the start of the page.
If the node is located on the first database page (it is the root node of the schema tree), there will be a file header at offset 0, after which the node header is placed.
The indirection vector is located right after the header(s).
Its purpose is to keep track of the offset of every cell on the page, sorted by key.
Each time a cell is added to or removed from the node, the indirection vector is updated.
Indirection vector values are contiguous and fixed-length, making binary search simple and quick.
For an empty node (the indirection vector has 0 length), the gap space will occupy everything from after the headers to the end of the page.
When the first cell is added, the indirection vector grows by the size of a cell pointer, and the cell itself is written at the end of the gap space (flush with the end of the page).
Like the first cell, additional cells are written at the end of the gap space, shrinking the gap space and growing the cell content area and indirection vector.
When the node can no longer fit another cell, it overflows, and the tree must be rebalanced to make room.
Also, nodes (besides the root when the tree is empty) are not allowed to become empty.
If a node becomes empty, it is considered to be "underflowing".
Rebalancing is required to restore the tree invariants.

A free block is created each time a cell is erased from a node.
Free blocks are maintained as an intrusive list and threaded through the node.
Each free block must store its own size, as well as the offset of the next free block (or 0 if there is not another free block).
This means that a free block can be no less than 4 bytes in length.
If less than 4 bytes are released back to the node, there will not be enough room to store the intrusive list information.
In this case, the memory region becomes a fragment.
The total size of all fragments on a page is kept in the node header.
When a free blocks is created, an attempt is made to merge it with an adjacent free block, consuming intervening fragments if possible.
Note that fragments are only created when only part of a free block is used to fulfill an allocation, and less than 4 bytes are left over.
Cell headers are padded to 4 bytes, so the smallest possible cell will become a free block.

#### Rebalancing
The `Tree` class is responsible for making sure that it is balanced before returning from either `put()` or `erase()`.
This is accomplished by calling one of the two rebalancing routines, `resolve_overflow()` or `resolve_underflow()`.
These routines, in turn, call other subroutines as outlined in the following table:

| Caller                | Callee                 | Note                               |
|:----------------------|:-----------------------|:-----------------------------------|
| `resolve_overflow()`  | `split_root()`         | Increases the height of the tree   |
| `resolve_overflow()`  | `split_nonroot()`      | Adds a node to the tree            |
| `resolve_underflow()` | `fix_root()`           | Decreases the height of the tree   |
| `resolve_underflow()` | `fix_nonroot()`        | May remove a node from the tree    |
| `split_nonroot()`     | `split_nonroot_fast()` | Optimization for sequential writes |
| `split_nonroot()`     | `redistribute_cells()` | *See below*                        |
| `fix_nonroot()`       | `redistribute_cells()` | *See below*                        |

As shown above, there are several routines that call `redistribute_cells()`.
`redistribute_cells()` expects 2 sibling nodes, 1 of which must be empty and the other nonempty, and their parent.
It works by attempting to transfer roughly half of the cell content from the nonempty node to the empty node.
If there are not enough cells in the nonempty node, then the two nodes are merged.

When a node is split in `redistribute_cells()`, a pivot is posted to the parent to separate the left and right siblings.
Similarly, when a merge occurs in `fix_nonroot()/redistribute_cells()`, the pivot that had previously separated the two nodes is transferred down into the merged child.
If the parent node is overflowing after `redistribute_cells()`, an additional call to a `split_*()` is needed.
Likewise, if the parent is underflowing, a `fix_*()` must be called.

If the node that needs rebalancing is the root node, one of the `*_root()` subroutines is called.
`split_root()` is called when the root node overflows.
It works by transferring its contents to a new node, and setting this node as a child of the (now empty) root.
This increases the height of the tree by 1.
`split_root()` must always be followed by a call to `split_nonroot()`, during which the child is split and a pivot posted to the root.
`fix_root()` is called when the root becomes empty.
An empty root node will always have a single child, unless the whole tree is empty.
The contents of the child node are copied into the empty root, and the child node is pushed to the freelist.
This decreases the height of the tree by 1.
When `fix_root()` is called on the [schema tree](#schema)

Suffix truncation is performed when an external node is split.
The pivot that is posted only needs to be long enough to direct traversals toward the correct external node.
For example, if the largest key in the left node is "AABCDD" and the smallest key in the right node is "AABDAA", the pivot can be chosen as "AABD".

#### Overflow chains
CalicoDB supports very long keys and values.
When a key or value is too large to fit on a page, some of it is transferred to an overflow chain.
An overflow chain is a singly-linked list of pages, each containing some portion of a record payload.
Each overflow chain page takes the following form:

| Offset | Size   | Name    | Purpose                                                          |
|:-------|:-------|:--------|:-----------------------------------------------------------------|
| 0      | 4      | NextPtr | Page ID of the next overflow chain page, or 0 if this is the end |
| 4      | PgSz-4 | Payload | Up to PgSz-4 bytes of record payload                             |

The formula for determining when a record should spill onto an overflow chain was modified from one found in SQLite3.
We try to keep the whole key embedded in the node, if possible.
This makes comparisons faster, since we don't have to read overflow pages to get the whole key.
Otherwise, the maximum local cell size (the amount of record stored locally, plus the cell header) is limited such that at least 4 cells will fit on any non-root page.
The root page may only hold 3 cells due to the space occupied by the file header.

### Freelist
Sometimes, database pages end up becoming unused.
This happens, for example, when a record with an overflow chain is erased.
Unused database pages are managed using the freelist.
There are 2 types of freelist pages: trunks and leaves.
Like overflow chain pages, freelist trunk pages form a linked list threaded through the database file.
Each trunk page contains the following information:

| Offset | Size   | Name    | Purpose                                              |
|:-------|:-------|:--------|:-----------------------------------------------------|
| 0      | 4      | NextPtr | Page ID of the next freelist trunk page              |
| 4      | 4      | LeafCnt | Number of freelist leaf page IDs stored on this page |
| 8      | PgSz-8 | Leaves  | `LeafCnt` leaf page IDs                              |

The last trunk page has its `NextPtr` field set to 0.
Freelist leaf pages contain no pertinent information.
They are not written to the WAL, nor are they stored in the pager cache.

### Pointer map
The pointer map is a collection of pointer map entries for every non-pointer map page in the database.
Each pointer map entry consists of the following fields:

| Offset | Size   | Name     | Purpose                                   |
|:-------|:-------|:---------|:------------------------------------------|
| 0      | 1      | Type     | Page type (see [pager.h](../src/pager.h)) |
| 1      | 4      | BackPtr  | Page ID of the page's parent              |

The pointer map is spread out over 0 or more specific database pages called pointer map pages.
The first pointer map page is always on page 2, that is, the page right after the root page.
Each pointer map page holds `page_size / 5` entries: one for each of the `page_size / 5` pages directly following it.
Every other pointer map page is located on the page following the last page referenced by the previous pointer map page.

Each cell that is moved between internal tree nodes must have its child's parent pointer updated.
If parent pointers are embedded in the nodes, splits and merges become very expensive, since each cell that is transferred requires the child page to be updated.
Pointer maps make splits and merges much more efficient by consolidating many parent pointers on a single page.

Pointer maps also help with the vacuum operation.
When the database is vacuumed, all freelist pages are collected at the end of the file.
This requires us to be able to swap any other database page with a freelist page.
Of course, each database page has 1 or more other pages that reference it, so each of these references must be updated to point to the new location.
Using the pointer map, we can locate any page's "parent" page.
Parent page types are given in the following table.
Note that there is no distinction between internal and external nodes here: that information is stored in the page header.
`kTreeNode` and `kTreeRoot` can be either internal or external, and `kTree*` refers to any tree node.

| Page type        | Parent page type                       |
|:-----------------|:---------------------------------------|
| `kTreeNode`      | `kTree*`                               |
| `kTreeRoot`      | `kEmpty`<sup>1</sup>                   |
| `kOverflowHead`  | `kTree*`                               |
| `kOverflowLink`  | `kOverflowHead`                        |
| `kFreelistTrunk` | `kFreelistTrunk`, `kEmpty`<sup>1</sup> |
| `kFreelistLeaf`  | `kFreelistTrunk`                       |

<sup>1</sup> `kEmpty` means "no parent".

### Schema
The schema is used to keep track of all trees in the database.
It is represented on disk by the tree rooted on the first database page.
This tree, the schema tree, is created when the database itself is created, and can never be dropped.
It stores the name and root page ID of every other tree, as well as other per-tree attributes.

### WAL
The WAL is based off of SQLite's WAL design.

#### WAL file
As described in [Architecture](#architecture), a database named `~/cats` will store its WAL in a file named `~/cats-wal`.
This file, hereafter called the WAL file, is opened when the first connection is made to `~/cats` (a connection is just an open `DB *` in some executing program).
The WAL file consists of a fixed-length header, followed by 0 or more WAL frames.
Each WAL frame contains a single database page, along with some metadata.
The WAL file is removed from disk when the last connection closes.

Most writes to the WAL are sequential, the exception being when a page is written out more than once within a transaction.
In that case, the most-recent version of the page will be overwritten.
This prevents long-running transactions from causing the WAL to grow very large.

#### shm file
Since the database file is never written during a transaction, its contents quickly become stale.
This means that pages existing in the WAL must be read from the WAL, not the database file.
The shm file is used to store the WAL index data structure, which provides a way to quickly locate pages in the WAL.
We also use the shm file to coordinate locks on the WAL.

## Error handling
`Status` objects are used describe errors in CalicoDB.
If an operation can fail, it will always return, or otherwise expose, a `Status` object.
If a system call fails during a read-write transaction, CalicoDB will refuse to perform any more work until the transaction has been aborted (the transaction handle has been `delete`ed).
Otherwise, any routine that fails can be retried any number of times.

## Database file format
The database file consists of 0 or more fixed-size pages.
A freshly-created database is just an empty database file.
When the first tree is created, the first 3 database pages are initialized in-memory.
The first page in the file, called the root page, contains the file header and serves as the [schema tree's][#schema] root node.
The second page is always a [pointer map](#pointer-map) page.
The third page is the root node of the newly-created tree.
As the database grows, additional pages are allocated by extending the database file.

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

## References
1. Graefe, Modern B-Tree Techniques, Foundation and Trends R<sup>©</sup> in Databases, vol 3, no 4, pp 203–402, 2010
2. Rudolf Bayer and Karl Unterauer. 1977. Prefix B-trees. ACM Trans. Database Syst. 2, 1 (March 1977), 11–26. https://doi.org/10.1145/320521.320530

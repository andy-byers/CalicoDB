# CalicoDB Design
CalicoDB is designed to be very simple to use.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.

## Architecture
CalicoDB uses a B<sup>+</sup>-tree with a write-ahead log (WAL).
The B<sup>+</sup>-tree containing the record store is located in a file with the same name as the database.
By default, the WAL file is named the same as the database, but with a suffix of `-wal`.
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
[//]: # (TODO)

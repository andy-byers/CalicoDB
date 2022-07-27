# Calico DB Design

**NOTES**:
+ If we encounter a corrupted WAL record during recovery, we need to check its LSN and the database flushed LSN.
If the corrupted record has an LSN >= the flushed LSN, we cannot know if the data on the page is correct, since we have committed possibly corrupted data.
We could check if the page contents match the "after" contents of the record, but it doesn't really matter since the record is corrupted anyway.
If there are WAL records after the corrupted record, we cannot roll back

## Terms
+ **block**: The basic unit of storage in the `wal` file (see **page**).
+ **cell**: An structure embedded within a **node** that contains a **key** and optionally a **value**.
  **Cells** can be either internal cells or external cells, depending on what type of node they belong to.
+ **cell directory**: An embedded array that is placed right after the node header.
  Maps **cell indexes** to **cell pointers**, and is kept sorted by the **keys**.
+ **cell index**: The index of a cell in the cell directory.
+ **cell pointer**: A 16-bit unsigned integer that describes the offset of a cell from the start of a node page.
+ **key**: A sequence of bytes that uniquely identify a record in the database.
  **Keys** are compared as if they are made up of unsigned bytes.
+ **link**: A page that contains a pointer to another page and optionally some data.
+ **node**: A page that contains cells and is involved in the B<sup>+</sup>-tree structure.
+ **overflow chain**: A linked list composed of link pages.
  Used to keep track of one or more pages, or part of a **value** that cannot fit in a **node**.
+ **page**: The basic unit of storage in the `data` file; every access attempts to read or write an entire page of data.
+ **page ID**: A 32-bit unsigned integer that uniquely identifies a page in the `data` file.
  **Page IDs** start at 1 and increase sequentially, with 0 being reserved for `NULL`.
+ **page pointer**: Synonym for **page ID**.
+ **value**: A sequence of bytes associated with a unique **key** to form a database record.

## Error Handling
Calico DB enforces certain rules to make sure that the database stays consistent through crashes and other exceptional events.
The current policy is to lock the database if an error is encountered while working with a writable page after the database has been modified during a transaction.
This is more restrictive than necessary, but has the benefit of covering all cases where the database could be corrupted.
The lock can be released by performing a successful abort operation.
Errors that result from passing invalid arguments to a method will never lock up the database.

## B<sup>+</sup>-Tree
Calico DB uses a dynamic-order B<sup>+</sup>-tree to maintain an ordered collection of records on-disk.
The tree is contained in the `data` file and is made up of two types of nodes: internal and external.
Nodes that have children are internal nodes, while those that do not are external nodes.
External nodes make up the lowest level of the B<sup>+</sup>-tree and are connected as a doubly-linked list.

### Pages
The page size is chosen at the time the database is created and must be a power of two.
All pages begin with a page header, except for the root page, which places the database header first.
Following the page header are the page contents, which depend on the page type.
Pages can be one of two general types, links or nodes, depending on the value of the `Type` field.
See [Links](#links) and [Nodes](#nodes) for more details.

### Links
A link is a page that holds a single pointer to another page.
They are used to form the freelist, and to connect overflow chains.

## Buffer Pool
Steal, no-force.

### Frames
TODO

### Pager
TODO

### Page Cache
Simplified 2Q replacement.

## Filesystem Interface
TODO

### Directories
TODO

### Files
Currently, the `data`, `log`, and `wal` files are maintained in the toplevel directory.
The `data` file contains the data records embedded in a B<sup>+</sup>-tree.
The `log` file contains the log output of the database instance, written by `spdlog`.
The `wal` file contains the write-ahead log.

### Nodes
Nodes are pages that participate in forming the B<sup>+</sup>-tree structure.
Each node contains many cells and is responsible for their local ordering as well as maintaining links to neighboring nodes.
Nodes are made up of three main regions: the header, the cell pointer list, and the cell content area.

[//]: # (TODO: We could probably do away with the free block count field and just use a "null-terminator" in the
               last free block. We may also be able to use a byte for the fragment count and defragment if it is
               about to overflow. It generally doesn't get that large anyway, it's just a measure to make sure we
               keep track of every byte on the page.)

#### Cell Directory
The cell directory is an embedded array located directly after the node header.
It stores the offset of each cell from the start of the node, ordered by the keys, which are stored with their respective cells further down in the cell content area.
We can use binary search on the cell directory to retrieve the location of a cell in $O(log_2 N)$, where $N$ is the number of cells in the node.

#### Cell Content Area
The cell content area makes up the majority of the page, and is located right after the cell directory.
As cells are added, the cell directory grows toward the end of the page.
The cells themselves are added to the cell content area from the reverse direction, such that the two regions meet in the middle.
When a cell is removed, its memory is added to a list of free blocks embedded within the node.
The number of free blocks and the location of the first free block are kept in the node header (`Free count` and `Free start`, respectively).
When a new cell is added, the free block list is traversed to see if there is a free block large enough to satisfy the request.
If so, the new cell is allocated from the free block. 
Otherwise, the cell is allocated from the gap region between the cell directory and the existing cell content.
As cells are added and removed, we may encounter a situation where the memory we need to write a new cell exists on the page, but is not contiguous.
Such a page requires defragmentation, where all free blocks and fragments are merged into the gap space.

## Free List
Occasionally, a routine will require deletion of a page.
The free list keeps track of these pages in a singly-linked list of free list link pages.
Each time we need to delete a page, we convert it to a link page and add it at the head of the list, which is stored in the database header.

## Overflow Chains
Currently, Calico DB allows insertion of arbitrary values, but places a limit on the key size.
If a record is too large, the excess portion of the value is copied to one or more overflow pages.
The cell made from this record will then store the first page ID in the overflow chain in its `Overflow ID` header field.
Note that no part of the key is ever transferred to an overflow page: every key must be entirely embedded in the node that it belongs to.
This makes traversing the B<sup>+</sup>-tree easier (we don't have to read additional pages to find keys) and doesn't mess up the page cache.

[//]: # (TODO: Now that we're using a simplified 2Q page cache, the cache won't get messed up unless we read the page 
               again while it is in the FIFO queue. We should be able to have arbitrary length keys, potentially spanning
               multiple overflow pages, without affecting the cache too much.)

## Write-Ahead Log (WAL)
Currently, the WAL consists of a single file, written in block-sized chunks.
It is managed by a pair of constructs: one to read from the file and one to write to it.
Both constructs operate on WAL records and perform their own caching internally.

TODO: Add info about the WALManager.

### WAL Records
WAL records are used to store information about updates made to database pages.
See ***3*** for more information about WAL records.
We use a similar scheme including multiple record types.
Basically, multiple WAL records can correspond to a single page update.
This is because WAL records can be split up into multiple blocks, depending on their size and the amount of memory remaining in the WAL writer's internal buffer.

### WAL Writer
The WAL writer fills up an internal buffer with WAL records.
When it runs out of space, it flushes the buffer to the WAL file.
At that point, all database pages with updates corresponding to the flushed records can be safely written to the `data` file.
To this end, we always keep the LSN of the most-recently-flushed record in memory.
Also note that the WAL is truncated after each commit, including when the database instance is closed.

### WAL Reader
The WAL reader is a cursor-like object used to traverse the `wal` file.
It can be used to traverse in either direction, but must always start at the beginning of the file.

## Cursors
A cursor acts much like an STL iterator, allowing traversal of the database in-order.
Since we are using a B<sup>+</sup>-tree, all records are stored in external nodes.
Thus, cursors are confined to bottom row of the tree and move around using sibling node links.
A valid cursor is one that is positioned on an existing database record, while an invalid cursor is one that is not.
Invalid cursors make no distinction between "end" (one past the last element) and the "rend" (one before the first element) positions.
When a cursor moves out of range, it is marked invalid and cannot be restored.
This is achieved by letting the cursor stay on the first or last element, and setting a boolean flag to mark invalid-ness.
While this prohibits us from achieving an ordering between any two cursors, we can still use equality comparison (with the caveat that all invalid cursors compare the same).
This means that iterating toward an invalid cursor is equivalent to iterating to the end of the database.
For example, the following snippet will iterate through the whole database.
```C++
namespace cco = calico;

// Here we assume that the database is nonempty and does not contain "xyz", or anything that compares 
// greater than it.
const auto bounds = db.find(cco::stob("xyz"), true);
assert(not bounds.is_valid());

for (auto c = db.find_minimum(); c != bounds; c++) {
    // Do something with c.key() and/or c.value().
}
```

This is effectively the same as iterating while `c.is_valid()`.

## Transactions
Calico DB uses transactions to preserve database integrity.
Transactions are atomic, that is, the effects of a transaction are either completely applied, or entirely discarded.
This is achieved by storing both undo and redo information in the WAL, and always flushing WAL records before their corresponding data pages.
In the event of a crash, we can always read the WAL forward to regain lost updates, or backward to undo applied updates.

## Recovery
Each time the database is closed, we commit the running transaction, which involves truncating the WAL.
This means that the presence of records in the WAL on startup indicates that the previous database instance had crashed, meaning recovery is necessary.
In Calico DB, recovery is pretty simple.
First, we read the WAL starting from the beginning, applying updates to stale pages.
If we encounter a commit record at the end of the WAL, then recovery is complete.
Otherwise, we do not have enough information to complete the transaction and must abort.
Here we read the WAL in reverse, reverting pages to their state before the transaction started.
In either case, we truncate the WAL and flush the buffer pool on completion.

[//]: # (TODO: Since we are currently using exceptions, we need to make sure that commit\(\), abort\(\), and
               the recovery procedure are all reentrant. They should be already, more or less, but we need
               explicit verification of this fact. If the recovery procedure throws, we will end up aborting
               the database constructor, but we should be able to try to construct that database instance as
               many times as we want until it maybe succeeds, and not have it become corrupted.)

## References
1. https://cstack.github.io/db_tutorial/
  + Awesome tutorial on database development in C
2. https://www.sqlite.org/arch.html
  + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
  + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
2. https://github.com/google/leveldb
  + The idea for using a hand-rolled slice object and some of its functionality are from `slice.h`.
  + Their CMake build process was very instructive.
3. https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
  + Nice explanation of RocksDB's WAL
  + The idea to have multiple different record types and to use a "tail" buffer are from this document
4. https://arpitbhayani.me/blogs/2q-cache
  + Nice description of the 2Q cache replacement policy

# Calico DB Design

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

## B<sup>+</sup>-Tree
Calico DB uses a B<sup>+</sup>-tree to maintain an ordered collection of keys and values.
The tree is contained in the `data` file and is made up of two types of nodes: internal and external.
Nodes that have children are internal nodes, while those that do not are external nodes.
External nodes make up the lowest level of the B<sup>+</sup>-tree and are connected as a doubly-linked list.

### Database Header
The database header is located at the start of the `data` file.
It contains important information that applies to the entire database.

| Size | Offset | Name        |
|-----:|-------:|:------------|
|    4 |      0 | Magic code  |
|    4 |      4 | Header CRC  |
|    4 |      8 | Page count  |
|    4 |     12 | Node count  |
|    4 |     16 | Free count  |
|    4 |     20 | Free start  |
|    2 |     24 | Page size   |
|    2 |     26 | Block size  |
|    4 |     28 | Key count   |
|    4 |     32 | Flushed LSN |
|   12 |     36 | Reserved    |

### Pages
The page size is chosen at the time the database is created and must be a power of two.
All pages begin with a page header, except for the root page, which places the database header first.
Following the page header are the page contents, which depend on the page type.
Pages can be one of two general types, links or nodes, depending on the value of the `Type` field.
See [Links](#links) and [Nodes](#nodes) for more details.
If the database instance was created with transactions enabled, pages will keep track of their contents before and after all modifications.
This is accomplished by copying the "before" page contents to scratch memory once, then updating the page in-place.
The offset and size of each write are kept track of, and a simple algorithm is used to keep them consolidated.
When the page is no longer needed, these updates are sent to the WAL.

#### Page Header
|  Size | Offset | Name    |
|------:|-------:|:--------|
|     4 |      0 | LSN     |
|     2 |      4 | Type    |

### Links
A link is a page that holds a single pointer to another page.
They are used to form the freelist, and to connect overflow chains.

#### Link Header
| Size | Offset | Name    |
|-----:|-------:|:--------|
|    4 |      0 | Next ID |

## Buffer Pool
TODO

### Frames
TODO

### Pager
TODO

### Page Cache
TODO

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

#### Node Header
The node header contains information about the node's layout and its connections to surrounding nodes.

| Size | Offset | Name                           |
|-----:|-------:|:-------------------------------|
|    4 |      0 | Header CRC                     |
|    4 |      4 | Parent ID                      |
|    4 |      8 | Rightmost child ID<sup>1</sup> |
|    4 |      8 | Right sibling ID<sup>1</sup>   |
|    4 |     12 | Left sibling ID<sup>2</sup>    |
|    2 |     14 | Cell count                     |
|    2 |     16 | Free block count               |
|    2 |     18 | Cell start                     |
|    2 |     20 | Free block start               |
|    2 |     22 | Fragment count                 |

<sup>1</sup> The rightmost child ID and right sibling ID fields refer to the same data location.
Use the rightmost child ID name in internal nodes and the right sibling ID name in external nodes.
<sup>2</sup> The left sibling ID, like the right sibling ID, is only present in external nodes.
This field is currently unused in internal nodes.

#### Cell Directory
The cell directory is located directly after the node header and contains the offset of each cell in the node.
This list is kept sorted by the keys, which are stored with their respective cells further down in the cell content area.
We can use binary search on the cell directory to retrieve a cell from the node in $O(log_2 N)$, where $N$ is the number of cells in the node.

#### Cell Content Area
After the cell directory is the cell content area.
As cells are added, the cell directory grows toward the end of the page.
The cells themselves are added to the cell content area from the reverse direction, such that the two regions meet in the middle.
When a cell is removed, its memory is added to a list of free blocks embedded within the node.
The number of free blocks and the location of the first free block are kept in the node header (`Free count` and `Free start`, respectively).
When a new cell is added, the free block list is traversed to see if there is a free block large enough to satisfy the request.
If so, the new cell is allocated from the free block. 
Otherwise, the cell is allocated from the gap region between the cell directory and the existing cell content.
As cells are added and removed, we may encounter a situation where the memory we need to write a new cell exists on the page, but is not contiguous.
Such a page requires defragmentation, where all free blocks and fragments are merged into the gap space.

### Cell Layout
| Size |     Offset | Name                      |
|-----:|-----------:|:--------------------------|
|    4 |          0 | Left child ID<sup>1</sup> |
|    2 |          4 | Key size (K)              |
|    4 |          6 | Value size (V)            |
|    K |         10 | Key                       |
|    L |     10 + K | Local value<sup>2</sup>   |
|    4 | 10 + K + L | Overflow ID<sup>2</sup>   |

<sup>1</sup> This field is only present in internal cells. <br>
<sup>2</sup> `Local value` (length = `L`) refers to the portion of the value stored directly in the node.
`L` depends on the combined size of the key and value and may or may not be equal to `V`.
See [Overflow Chains](#overflow-chains) for details.

## Free List
Occasionally, a routine will require deletion of a page, and since we are operating on a disk file, we need to manage these pages manually.
The current solution is to add them to a singly-linked list, called the free list.
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
The WAL design was inspired by that of Rocks DB, but may or may not be all that similar in reality.
It was mostly based off of some documents found in their GitHub wiki documentation (https://github.com/facebook/rocksdb/wiki/).
Currently the WAL consists of a single file, written in block-sized chunks.
It is managed by a pair of constructs: one to read from the file and one to write to it.
Both constructs operate on WAL records and perform their own caching internally.

### WAL Writer
The WAL writer fills up an internal buffer with WAL records.
When it runs out of space, it flushes the buffer to the WAL file.
At that point, all database pages with updates corresponding to these WAL records can be safely flushed to the `data` file.
To this end, we always keep the LSN of the most-recently-flushed record in memory.
Also note that the WAL is truncated after each commit, including when the database instance is closed.

### WAL Reader
The WAL reader is a cursor-like object used to traverse the `wal` file.
It can be used to traverse in either direction, but must always start on the first WAL record.

### WAL Records
WAL records are used to store information about updates made to database pages.
See the aforementioned Rocks DB wiki link for more information about WAL records.
We use a similar scheme including multiple record types.
Basically, multiple WAL records can correspond to a single page update.
This is because WAL records can be split up into multiple blocks, depending on their size and the amount of memory remaining in the WAL writer's internal buffer.

### WAL Record Header
| Size | Offset | Name             |
|-----:|-------:|:-----------------|
|    4 |      0 | LSN              |
|    4 |      4 | CRC              |
|    1 |      8 | Type             |
|    2 |      9 | Payload size (X) |
|    X |     11 | payload          |

### WAL Record Payload Header
| Size | Offset | Name              |
|-----:|-------:|:------------------|
|    4 |      0 | Previous LSN      |
|    4 |      4 | Page ID           |
|    2 |      6 | Count<sup>1</sup> |
<sup>1</sup> Determines the number of WAL record payload entries.

### WAL Record Payload Entry
Holds a region of a database page before and after some modification.

| Size | Offset | Name     |
|-----:|-------:|:---------|
|    2 |      0 | Offset   |
|    2 |      2 | Size (Y) |
|    Y |      4 | Before   |
|    Y |  4 + Y | After    |

## Cursors
A cursor acts much like an STL iterator, allowing traversal of the database in-order.
Since we are using a B<sup>+</sup>-tree, all records are stored in external nodes.
Thus, cursors are confined to bottom row of the tree and move around using sibling node links.
A valid cursor is one that is positioned on an existing database record.
An invalid cursor is one that is not.
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
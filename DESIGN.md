# Calico DB Design

## B-Tree
Calico DB uses a B-tree to maintain an ordered collection of keys and values.
The B-tree is made up of two types of nodes: internal and external.
Nodes that have children are internal nodes, while those that do not are external nodes.
External nodes make up the lowest level of the B-tree and are connected as a singly-linked list.

### Database Header
The database header is located at the start of the database file.
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
Pages are the basic unit of storage that make up the database file.
Every time we access the database file, it is to either read or write an entire page of data.
The page size is chosen at the time the database is created and must be a power of two.
Note that pages are similar to, but not the same as, blocks.
When we talk about blocks, we are referring to the same concept, but with respect to the WAL file rather than the databasee file.
All pages begin with a page header, except for the root page, which places the database header first.
Following the page header are the page contents, which depend on the page type.
Pages can be one of two types, links or nodes, depending on the value of the `Type` field.

#### Page Header
|  Size | Offset | Name    |
|------:|-------:|:--------|
|     4 |      0 | LSN     |
|     2 |      4 | Type    |

### Links
A link is a page that holds a single pointer to another page.
They are used to form singly-linked lists for the freelist and overflow values.

#### Link Header
| Size | Offset | Name    |
|-----:|-------:|:--------|
|    4 |      0 | Next ID |

### Nodes
Nodes are pages that participate in forming the B-tree structure.
Each node contains many cells and is responsible for their local ordering as well as maintaining links to neighboring nodes.
Nodes are made up of three main regions: the header, the cell pointer list, and the cell content area.

#### Node Header
The node header contains information about the node's layout and its connections to surrounding nodes.

| Size | Offset | Name                |
|-----:|-------:|:--------------------|
|    4 |      0 | Header CRC          |
|    4 |      4 | Parent ID           |
|    4 |      8 | Rightmost child ID¹ |
|    4 |      8 | Right sibling ID¹   |
|    2 |     12 | Cell count          |
|    2 |     14 | Free block count    |
|    2 |     16 | Cell start          |
|    2 |     18 | Free block start    |
|    2 |     20 | Fragment count      |

¹ The rightmost child ID and right sibling ID fields refer to the same data location.
Use the rightmost child ID name in internal nodes and the right sibling ID name in external nodes.

#### Cell Directory
The cell directory is located directly after the node header and contains the offset of each cell in the node.
This list is kept sorted by the keys, which are stored with their respective cells further down in the cell content area.
We can use binary search on the cell directory to retrieve a cell from the node in $O(log_2 N)$, where $N$ is the number of cells in the node.

#### Cell Content Area
After the cell directory comes the cell content area.
As cells are added, the cell directory grows toward the end of the page.
The cells themselves are added to the cell content area from the reverse direction, such that the two regions meet in the middle.
When a cell is removed, its memory is added to a list of free blocks embedded within the node.
The number of free blocks and the location of the first free block are kept in the node header (`Free count` and `Free start`, respectively).
When a new cell is added, the free block list is traversed to see if there is a free block large enough to satisfy the request.
If so, the new cell is allocated from the free block. 
Otherwise, the cell is allocated from the gap region between the cell directory and the existing cell content.

### Cell Layout
| Size |     Offset | Name           |
|-----:|-----------:|:---------------|
|    4 |          0 | Left child ID¹ |
|    2 |          4 | Key size (K)   |
|    4 |          6 | Value size (V) |
|    K |         10 | Key            |
|    L |     10 + K | Local value²   |
|    4 | 10 + K + L | Overflow ID²   |

¹ This field is only present in internal cells.
² `Local value` (length = `L`) refers to the portion of the value stored directly in the node.
`L` depends on the combined size of the key and value and may or may not be equal to `V`.
See [Overflow Chains](#overflow-chains) for details.

## Free List
TODO

## Overflow Chains
Currently, Calico DB allows insertion of arbitrary values, but places a limit on the key size.
If a record has total payload size that is greater than what is allowed, the excess portion of the value field is copied to one or more overflow pages.
The cell made from this record will then store the first page ID in the overflow chain in its `Overflow ID` header field.
Note that no part of the key is ever transferred to an overflow page: every key must be entirely embedded in the node that it belongs to.
This makes traversing the B-tree easier (we don't have to read additional pages to find keys) and doesn't mess up the page cache.

## Write-Ahead Log (WAL)
TODO

## Buffer Pool
TODO

## Filesystem Interface
TODO
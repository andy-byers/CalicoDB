
### Glossary
+ **internal node**: A B-tree node that has children, a.k.a. a branch node
+ **external node**: A B-tree node that has no children, a.k.a. a leaf node
+ **page**: A fixed-sized chunk of data read from the database file (see [Pages](#pages))
+ **block**: A fixed-sized chunk of data read from the WAL file
+ **cell**: The basic unit of storage in the B-tree (see [Cells](#cells))
+ **overflow cell**: A cell that cannot fit in a node and must be stored elsewhere temporarily

### Database Header
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
The page size is chosen at the time a database is created and must be a power of two.
Note that pages are similar to, but not the same as, blocks.
When we talk about blocks, we are referring the same concept, but with respect to the WAL file rather than the databasee file.
All pages begin with a page header, except for the root page, which places the file header first.
Following the page header are the page contents, which depend on the page type.
Pages can be one of two types, links or nodes, depending on the value of the `Type` field.

#### Page Header
|  Size | Offset | Name    |
|------:|-------:|:--------|
|     4 |      0 | LSN     |
|     2 |      4 | Type    |

### Links
A link is a page that holds a single pointer to another page.
They are used for both the freelist and to hold overflow values.

#### Link Header
| Size | Offset | Name    |
|-----:|-------:|:--------|
|    4 |      0 | Next ID |

### Nodes
Nodes are pages that participate in forming the B-tree structure.
Each node contains many cells and is responsible for their local ordering.
Nodes are made up of three main regions: the header, the cell pointer list, and the cell content area.

#### Node Header
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

#### Cell Pointer List
The cell pointer list contains the offset of each cell in the node.
This list is kept sorted by the keys, which are stored with their respective cells further down in the cell content area.
Cells can then be addressed using their index in the cell pointer list.
We can also use binary search on this sorted list to speed up searches.

#### Cell Content Area
As cells are added, the cell pointer list grows toward the end of the page.
The cells themselves are added to the cell content area from the reverse direction, such that the two regions meet in the middle.
When a cell is removed, its memory is added to a list of free blocks embedded within the node.
Then when additional cells are added, this list is traversed to see if there is a free block large enough to satisfy the request.

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

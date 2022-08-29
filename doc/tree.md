## Terms
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

### Nodes
Nodes are pages that participate in forming the B<sup>+</sup>-tree structure.
Each node contains many cells and is responsible for their local ordering as well as maintaining links to neighboring nodes.
Nodes are made up of three main regions: the header, the cell pointer list, and the cell content area.

#### Cell Directory
The cell directory is an embedded array located directly after the node header.
It stores the offset of each cell from the start of the node, ordered by the keys, which are stored with their respective cells further down in the cell content area.
We can use binary search on the cell directory to retrieve the location of a cell in $O(log_2 N)$, where $N$ is the number of cells in the node.

#### Cell Content Area
The cell content area makes up the majority of the page, and is located right after the cell directory.
As cells are added, the cell directory grows toward the end of the page.
The cells themselves are added to the cell content area from the reverse direction, such that the two regions meet in the middle.
When a cell is removed, its memory is added to a list of free blocks embedded within the node.
The location of the first free block is kept in the node header (`Free start`).
The number of free blocks is not necessary to store: we use a "next pointer" of 0 to indicate that there are no more free blocks to follow.
This means that if the `Free start` is 0, the free block list is empty.
When a new cell is added, the free block list is traversed to see if there is a free block large enough to satisfy the request.
If so, the new cell is allocated from the free block.
Otherwise, the cell is allocated from the gap region between the cell directory and the existing cell content.
As cells are added and removed, we may encounter a situation where the memory we need to write a new cell exists on the page, but is not contiguous.
Such a situation requires page defragmentation, where all free blocks and fragments are merged into the gap space.

## Free List
Occasionally, a routine will require deletion of a page.
The free list keeps track of these pages in a singly-linked list of link pages.
Each time we need to delete a page, we convert it to a link page and add it at the head of the list, which is stored in the database header.
Like the free block list contained in each [node](#nodes), we don't store the free list length and instead use a NULL page ID to indicate the end of the list.

## Overflow Chains
Currently, Calico DB allows insertion of nearly arbitrary values (up to 4 GB), but places a hard limit on the key size.
If a record is too large, the excess portion of the value is copied to one or more overflow pages.
The cell made from this record will then store the first page ID in the overflow chain in its `Overflow ID` header field.
Note that no part of the key is ever transferred to an overflow page: every key must be entirely embedded in the node that it belongs to.
This makes traversing the B<sup>+</sup>-tree faster (we don't have to read additional pages, pages that are likely not already cached, to find keys).

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
In either case, we truncate the WAL and advance_block the block pool on completion.

# Optimizations?
+ We could come up with a physiological logging scheme to try and reduce the amount of data we write to the WAL

# Calico DB File Format

## `data` File
## `wal` File
## `log` File

### Database Header
The database header is located at the start of the `data` file.
It contains important database state information and is accessed by multiple components.

| Size | Offset | Name         | Description                                                                              |
|-----:|-------:|:-------------|:-----------------------------------------------------------------------------------------|
|    4 |      0 | Magic code   | An integer constant that identifies the `data` file as belonging to a Calico DB database |
|    4 |      4 | Header CRC   | CRC computed on this header                                                              |
|    4 |      8 | Page count   | Number of pages allocated to the database                                                |
|    4 |     12 | Node count   | Number of node pages allocated to the database                                           |
|    4 |     16 | Free count   | Number of pages in the free list                                                         |                                           
|    4 |     20 | Free start   | Page ID of the first free list page                                                      |
|    2 |     24 | Page size    | Size of a database page in bytes                                                         |
|    2 |     26 | Block size   | Size of a WAL block in bytes                                                             |
|    4 |     28 | Record count | Number of records in the database                                                        |
|    4 |     32 | Flushed LSN  | LSN of the last WAL record flushed to disk after a successful commit                     |
|   12 |     36 | Reserved     | Reserved for expansion                                                                   |

### Page Header
|  Size | Offset | Name    | Description                                                        |
|------:|-------:|:--------|:-------------------------------------------------------------------|
|     4 |      0 | LSN     | LSN of the WAL record describing the last modification to the page |
|     2 |      4 | Type    | Page type field                                                    |

### Link Header
| Size | Offset | Name    | Description                           |
|-----:|-------:|:--------|:--------------------------------------|
|    4 |      0 | Next ID | Page ID of the next page in the chain |

### Node Header
The node header contains information about the node's layout and its connections to surrounding nodes.

| Size | Offset | Name                           | Description                                      |
|-----:|-------:|:-------------------------------|:-------------------------------------------------|
|    4 |      0 | Header CRC                     | CRC computed on this header                      |
|    4 |      4 | Parent ID                      | Page ID of the parent node                       |
|    4 |      8 | Rightmost child ID<sup>1</sup> | Page ID of the rightmost child node              |
|    4 |      8 | Right sibling ID<sup>1</sup>   | Page ID of the right sibling node                |
|    4 |     12 | Left sibling ID<sup>2</sup>    | Page ID of the left sibling node                 |
|    2 |     14 | Cell count                     | Number of cells in the node                      |
|    2 |     16 | Free block count               | Number of free blocks in the node                |
|    2 |     18 | Cell start                     | Offset of the start of the "gap" area            |
|    2 |     20 | Free block start               | Offset of the first entry in the free block list |
|    2 |     22 | Fragment count                 | Number of fragmented bytes                       |

<sup>1</sup> The rightmost child ID and right sibling ID fields refer to the same data location.
Use the rightmost child ID name in internal nodes and the right sibling ID name in external nodes.
<sup>2</sup> The left sibling ID, like the right sibling ID, is only present in external nodes.
This field is currently unused in internal nodes.

### Cell Layout
| Size |     Offset | Name                      | Description                                     |
|-----:|-----------:|:--------------------------|:------------------------------------------------|
|    4 |          0 | Left child ID<sup>1</sup> | Page ID of the left child node                  |
|    2 |          4 | Key size (K)              | Size of the key in bytes                        |
|    4 |          6 | Value size (V)            | Size of the value in bytes                      |
|    K |         10 | Key                       | Key by which cells are ordered                  |
|    L |     10 + K | Local value<sup>2</sup>   | Some or all of the record value                 |
|    4 | 10 + K + L | Overflow ID<sup>3</sup>   | Page ID of the first page in the overflow chain |

<sup>1</sup> This field is only present in internal cells. <br>
<sup>2</sup> `Local value` (length = `L`) refers to the portion of the value stored directly in the node.
`L` depends on the combined size of the key and value and may or may not be equal to `V`. <br>
<sup>3</sup> This field is only present if the value is too large to fit in the node, i.e. $L < V$.
See [Overflow Chains](#overflow-chains) for details.

### WAL Record Header
| Size | Offset | Name         | Description                                       |
|-----:|-------:|:-------------|:--------------------------------------------------|
|    4 |      0 | LSN          | Unique log sequence number describing this record |
|    4 |      4 | CRC          | CRC computed on this record                       |
|    1 |      8 | Type         | WAL record type                                   |
|    2 |      9 | Payload size | Size of the payload in bytes                      |

### WAL Record Payload Header
| Size | Offset | Name              | Description                                    |
|-----:|-------:|:------------------|:-----------------------------------------------|
|    4 |      0 | Previous LSN      | LSN describing the previous update to the page |
|    4 |      4 | Page ID           | Page ID of the page that was updated           |
|    2 |      6 | Count<sup>1</sup> | Number of payload entries                      |

<sup>1</sup> Determines the number of WAL record payload entries

### WAL Record Payload Entry
Holds a region of a database page before and after some modification.

| Size | Offset | Name     | Description                                     |
|-----:|-------:|:---------|:------------------------------------------------|
|    2 |      0 | Offset   | Offset of the update from the start of the page |
|    2 |      2 | Size (Y) | Size of the updated region in bytes             |
|    Y |      4 | Before   | Contents before the update                      |
|    Y |  4 + Y | After    | Contents after the update                       |


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

### Page Header
| Size | Offset | Name | 
|-----:|-------:|:-----|
|    4 |      0 | LSN  |
|    2 |      4 | Type |

### Node Header
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

### Link Header
| Size | Offset | Name    | 
|-----:|-------:|:--------|
|    4 |      0 | Next ID |

### Cell Layout

#### External Cell Layout
| Size |    Offset | Name           |
|-----:|----------:|:---------------|
|    2 |         0 | Key size (K)   | 
|    4 |         2 | Value size (V) | 
|    K |         6 | Key            | 
|    L |     6 + K | Local value¹   | 
|    4 | 6 + V + L | Overflow ID¹   |

#### Internal Cell Layout
| Size |     Offset | Name           |
|-----:|-----------:|:---------------|
|    4 |          0 | Left child ID  | 
|    2 |          4 | Key size (K)   | 
|    4 |          6 | Value size (V) | 
|    K |         10 | Key            | 
|    L |     10 + K | Local value¹   | 
|    4 | 10 + K + L | Overflow ID¹   |

¹ `Local value` (length = `L`) refers to the portion of the value stored directly in the node.
`L` depends on the combined size of the key and value and may or may not be equal to `V`.
See [Overflow Chains](#overflow-chains) for details.
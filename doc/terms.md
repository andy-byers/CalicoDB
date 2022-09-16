## Terms 

+ **cell**: An structure embedded within a **node** that contains a **key** and possibly a **value**.
  **Cells** can be either internal cells or external cells, depending on what type of node they belong to.
+ **cell index**: The index of a cell in the cell directory.
+ **cell pointer**: A 16-bit unsigned integer that describes the offset of a cell from the start of a node page.
+ **key**: A sequence of bytes that uniquely identify a record in the database.
  **Keys** are compared lexicographically as unsigned bytes (i.e. std::memcmp() order).
+ **link**: A page that contains a pointer to another page and optionally some data.
+ **node**: A page that contains cells and is involved in the B<sup>+</sup>-tree structure.
+ **overflow chain**: A linked list composed of link pages.
  Used to keep track of one or more pages, or part of a **value** that cannot fit in a **node**.
+ **page**: The basic unit of storage in the database file; every access attempts to read or write an entire page of data.
+ **page ID**: A 64-bit unsigned integer that uniquely identifies a page in the database file.
  **Page IDs** start at 1 and increase sequentially, with 0 being reserved for `NULL`.
+ **value**: A sequence of bytes associated with a unique **key** to form a database record.


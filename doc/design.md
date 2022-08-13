# Calico DB Design

<p align="center">
  <img src="./architecture.png" width="400px" alt="Diagram of Calico DB's architecture"/>
</p>

## Components
Calico DB is made up of 6 major components:

| Component  | Purpose                                                                             |
|:-----------|:------------------------------------------------------------------------------------|
| `core`     | Main interface for interacting with database components                             |
| `tree`     | Responsible for organizing the storage of records on database pages                 |
| `pager`    | Responsible for organizing the storage of database pages in the database file       |
| `store`    | Provides storage for the database file, WAL segments, and possibly an info log file |
| `wal`      | Stores and retrieves records describing changes made to the database                |
| `cursor`   | Provides ordered, read-only access to database records.                             |

## Error Handling
Calico DB enforces certain rules to make sure that the database stays consistent through crashes and other exceptional events.
The current policy is to lock the database if an error is encountered while working with a writable page after the database has been modified during a transaction.
This is more restrictive than necessary, but has the benefit of covering more cases where the database could be corrupted.
The lock can be released by performing a successful abort operation.
Errors that result from passing invalid arguments to a method will never lock up the database.

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

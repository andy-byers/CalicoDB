# CalicoDB

> **Warning**: This library has not been code reviewed, and I'm not an expert database developer. 
> Please don't use it for anything serious!
> I'm open to comments/criticism/pull requests though, as I want to make CalicoDB a useful library eventually!

> **Note**: I've been messing with this library for a while now, and I think it's finally coming together design-wise!
> We have transactions, tables, cursors, vacuum, and multiversion concurrency control (MVCC).
> This branch (develop) is intended for working on testing and getting the MVCC to work.
> I'll merge it back into main intermittently so that changes are visible, but some concurrency-related things will likely be broken for a while.

CalicoDB is an embedded transactional key-value database written in C++17.
It exposes a small API that allows storage and retrieval of arbitrary byte sequences.
CalicoDB runs in a single thread and uses a B<sup>+</sup>-tree backend.
It is intended for read-heavy embedded applications.

![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

<div align="center">
    <img src="doc/mascot.png" style="width: 40%; max-width: 400px" />
</div>

+ [Features](#features)
+ [Caveats](#caveats)
+ [Documentation](#documentation)
+ [Dependencies](#dependencies)
+ [Performance](#performance)
+ [TODO](#todo)
+ [Contributions](#contributions)

## Features
+ Records are stored in tables, each with an independent range of keys
+ All tables are stored in the same file, similar to the SQLite database file format
+ Supports multiple readers and a single writer simultaneously
+ Per-table bidirectional iteration using cursors
+ Crash protection via write-ahead logging
+ Vacuum operation to reclaim unused memory while running
+ Various parameters can be tuned (cache size, etc.)

## Caveats
+ Database is single-threaded: all I/O runs in the main thread
+ Concurrent usage requires each connection to have its own `DB`
+ Platform must support shared-memory primitives
+ Will not work on network filesystems

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself has no dependencies.
The unit tests depend on `@google/googletest`, and the benchmarks depend on `@google/benchmark`.
Both are downloaded using during the build.

## Performance
Performance benchmarks can be found [here](./test/benchmarks).

## TODO
1. Get everything code reviewed!
2. Support Windows (write an `Env` implementation)
3. Modify the tree to not use sibling pointers, or at least don't keep a left sibling pointer
   + Causes extra pages to be written to the WAL during splits
4. Look into writing a more involved balancing routine
5. Better freelist that 2 types of pages: leaf and trunk pages
   + Leaf pages contain no data and don't have to be written to the WAL
   + Trunk pages contain the page IDs of many leaf pages

## Documentation
Check out CalicoDB's [usage and design documents](doc).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.
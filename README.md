# CalicoDB

> **Warning**: This library has not been code reviewed, and I'm not an expert database developer. 
> Please don't use it for anything serious!
> I'm open to comments/criticism/pull requests though, as I want to make CalicoDB a useful library eventually!

CalicoDB is an embedded key-value database written in C++17.
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
+ Bidirectional iteration using cursors (per-table)
+ Crash protection via write-ahead logging
+ Vacuum operation to reclaim unused memory while running
+ Various parameters can be tuned (page size, cache size, etc.)

## Caveats
+ Concurrency control must be provided externally
+ Random writes are pretty slow

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself has no dependencies.
The unit tests depend on `@google/googletest`, and the benchmarks depend on `@google/benchmark`.
Both are downloaded using during the build.

## Performance
CalicoDB uses a write-ahead log (WAL) implementation that is similar to SQLite's WAL.
This means that most writes and commits will not modify the database file at all: they just write to the WAL.
The WAL contents are written back to the database file during "checkpoint" operations.
Performance benchmarks can be found [here](./test/benchmarks).

## TODO
1. Get everything code reviewed!
2. Need to implement repair (`Status DB::repair()`)
    + Run when a database cannot be opened due to corruption (not the same as recovery)
3. Support Windows (write an `Env` implementation)
4. Modify the tree to not use sibling pointers, or at least don't keep a left sibling pointer
   + Causes extra pages to be written to the WAL during splits
5. Look into writing a more involved balancing routine
6. Better freelist that 2 types of pages: leaf and trunk pages
   + Leaf pages contain no data and don't have to be written to the WAL
   + Trunk pages contain the page IDs of many leaf pages

## Documentation
Check out CalicoDB's [usage and design documents](doc).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.
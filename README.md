# CalicoDB
CalicoDB is a tiny key-value database library written in C++17.
It exposes a small API that allows transactional storage and retrieval of byte sequences from multiple key spaces.
CalicoDB runs in a single thread and uses a B<sup>+</sup>-tree backend.
It is intended for read-heavy embedded applications.

![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/test.yml/badge.svg)

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Documentation](#documentation)
+ [Requirements](#requirements)
+ [Performance](#performance)
+ [TODO](#todo)
+ [Contributions](#contributions)

## Disclaimer
CalicoDB is under active development.
It is not ready for production.
The author is not a professional database developer, so any type of feedback is appreciated!

## Features
+ Records are stored in buckets, each with an independent range of keys
+ All buckets are stored in the same file, similar to the SQLite database file format
+ Supports multiple readers and a single writer simultaneously (readers have snapshot isolation)
+ Per-bucket bidirectional iteration using cursors
+ Crash protection via write-ahead logging
+ Live defragmentation is supported
+ In-memory databases

## Caveats
+ Each connection is single-threaded: all I/O runs in the main thread
+ Database page size is fixed at 4 KiB
+ Concurrent usage requires each connection to have its own `DB`
+ Only practical for datasets that can reasonably be stored in a single file 
+ Platform must support shared-memory primitives
+ Will not work on network filesystems

## Documentation
Check out the [docs](doc/doc.md).

## Requirements
The library requires the following to compile:
+ CMake version 3.14 or later
+ C++ compiler with support for C++17
  + GCC (>= 7.0)
  + Clang (>= 5.0)

The tests depend on `@google/googletest`, which is downloaded during the build.

## Performance
Performance benchmarks are run in a [fork of LevelDB](https://github.com/andy-byers/leveldb/tree/db_bench_calicodb).
Recent results can be found [here](https://github.com/andy-byers/leveldb/blob/db_bench_calicodb/benchmarks/results.md).

## TODO
1. Get everything code reviewed!
2. Support Windows (write an `Env` implementation)

## Documentation
Check out CalicoDB's [usage and design documents](doc).
Build instructions can be found [here](doc/doc.md#build).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.
# CalicoDB
CalicoDB is an embedded transactional key-value database written in C++17.
It exposes a small API that allows storage and retrieval of arbitrary byte sequences.
CalicoDB runs in a single thread and uses a B<sup>+</sup>-tree backend.
It is intended for read-heavy embedded applications.

![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/test.yml/badge.svg)

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Documentation](#documentation)
+ [Dependencies](#dependencies)
+ [Performance](#performance)
+ [TODO](#todo)
+ [Contributions](#contributions)

## Disclaimer
CalicoDB is in active development.
It is not ready for production.
The author is not a professional database developer, so any type of feedback is appreciated.

## Features
+ Records are stored in buckets, each with an independent range of keys
+ All buckets are stored in the same file, similar to the SQLite database file format
+ Supports multiple readers and a single writer simultaneously
+ Per-bucket bidirectional iteration using cursors
+ Crash protection via write-ahead logging
+ Live defragmentation is supported
+ In-memory databases

## Caveats
+ Database is single-threaded: all I/O runs in the main thread
+ Database page size is fixed at 4 KiB
+ Concurrent usage requires each connection to have its own `DB`
+ Only practical for datasets that can reasonably be stored in a single file 
+ Platform must support shared-memory primitives
+ Will not work on network filesystems

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself has no dependencies.
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
# CalicoDB

> **Warning**: This library is not quite stable, nor is it code reviewed. 
> Please don't use it for anything serious!

Calico DB is an embedded key-value database written in C++17.
It exposes a small API that allows storage and retrieval of variable-length byte sequences.

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
+ Bidirectional iteration using cursors
+ Crash protection via write-ahead logging
+ Variable-length keys and values (see [Caveats](#caveats))
+ Various parameters can be tuned (page size, cache size, etc.)

## Caveats
+ Maximum key length is anywhere from 101 B to ~8 KiB, depending on the chosen page size
+ Maximum value length is ~4 GiB
+ Concurrency control must be provided externally

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself depends on `@TartanLlama/expected`.
The tests depend on `@google/googletest`, and the benchmarks depend on `@google/benchmark`.
Dependencies are either downloaded using CMake's FetchContent API, or bundled with the source code.

## Performance
Calico DB is optimized for read-heavy workloads with intermittent batches of sequential writes.
Performance benchmarks are run in a modified version of LevelDB's benchmark suite, which can be found [here](https://github.com/andy-byers/leveldb/tree/db_bench_calico).
Here are some results from an analysis where CalicoDB was benchmarked against SQLite3 and TreeDB.
A commit was performed on the CalicoDB instance every 1,000 writes.

| Benchmark name           | CalicoDB result (ops/second) | SQLite3 result (ops/second) | TreeDB result (ops/second) |
|:-------------------------|-----------------------------:|----------------------------:|---------------------------:|
| `fillseq`<sup>*</sup>    |                      542,299 |                   1,326,260 |                  1,191,895 |
| `fillrandom`<sup>*</sup> |                      174,095 |                     189,681 |                    326,691 |
| `overwrite`<sup>*</sup>  |                       73,249 |                     173,461 |                    288,684 |
| `readrandom`             |                      458,505 |                     515,198 |                    413,907 |
| `readseq`                |                    2,369,668 |                  10,526,316 |                  3,690,037 |
| `fillrand100k`           |                          425 |                       5,215 |                     11,387 |
| `fillseq100k`            |                          285 |                       6,731 |                      9,560 |
| `readseq100k`            |                       14,292 |                      49,232 |                     65,557 |
| `readrand100k`           |                       17,509 |                      10,894 |                     66,028 |

<sup>*</sup> These benchmarks are affected by the fact that we only commit every 1,000 writes.
For this reason, the SQLite3 benchmarks actually list the results for the much faster batched versions, which commit every 1,000 writes (i.e. `fillseq` is actually `fillseqbatch` for SQLite3).

## TODO
1. Get everything code reviewed!
2. Need to implement compaction (`Status Database::vacuum()`)
    + We need some way to collect freelist pages at the end of the file so that we can truncate
    + Look into SQLite's pointer maps
3. Need to implement repair (`Status Database::repair()`)
    + Run when a database cannot be opened due to corruption (not the same as recovery)
4. Support Windows (write a `Storage` implementation)

## Documentation
Check out Calico DB's [usage and design documents](doc).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.
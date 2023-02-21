# CalicoDB

> **Warning**: This library is not quite stable, nor is it code reviewed. 
> Please don't use it for anything serious!

Calico DB is an embedded key-value database written in C++17.
It exposes a small API that allows storage and retrieval of arbitrary byte sequences.

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
+ Vacuum operation to reclaim unused pages while running
+ Various parameters can be tuned (page size, cache size, etc.)

## Caveats
+ Concurrency control must be provided externally
+ Multiple simultaneous transactions are not supported

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself has no dependencies.
The tests depend on `@google/googletest`, which is downloaded using CMake's FetchContent API.

## Performance
Calico DB is optimized for read-heavy workloads with intermittent batches of sequential writes.
Performance benchmarks are run in a modified version of LevelDB's benchmark suite, which can be found [here](https://github.com/andy-byers/leveldb/tree/db_bench_calico).
These results are from an analysis where CalicoDB was benchmarked against SQLite3 and TreeDB.
`db_bench` prints the following line describing the CPU and cache:
```
CPU:            16 * 12th Gen Intel(R) Core(TM) i5-12600K
CPUCache:       20480 KB
```

The CalicoDB instance was committed every 1,000 writes.
Only benchmarks relevant to CalicoDB are included.

| Benchmark name             | CalicoDB result (ops/second) | SQLite3 result (ops/second) | TreeDB result (ops/second) |
|:---------------------------|-----------------------------:|----------------------------:|---------------------------:|
| `fillseq`<sup>*</sup>      |                      488,759 |                   1,326,260 |                  1,191,895 |
| `fillrandom`<sup>*</sup>   |                      180,538 |                     189,681 |                    326,691 |
| `overwrite`<sup>*</sup>    |                      195,389 |                     173,461 |                    288,684 |
| `readrandom`               |                      514,933 |                     515,198 |                    413,907 |
| `readseq`                  |                    3,300,330 |                  10,526,316 |                  3,690,037 |
| `fillrand100k`<sup>*</sup> |                        3,483 |                       5,215 |                     11,387 |
| `fillseq100k`<sup>*</sup>  |                        3,733 |                       6,731 |                      9,560 |
| `readseq100k`              |                       22,196 |                      49,232 |                     65,557 |
| `readrand100k`             |                       21,497 |                      10,894 |                     66,028 |

<sup>*</sup> These benchmarks are affected by the fact that we use a batch size of 1,000.
The call to `Database::commit()` will flush pages from older transactions, advance the WAL to a new segment, and possibly remove obsolete WAL segments, so it has quite a bit of overhead.
For this reason, the SQLite3 benchmarks actually list the results for the much faster batched versions, which commit every 1,000 writes (i.e. `fillseq` is actually `fillseqbatch` for SQLite3).

Additional work will need to be done to reduce the overhead of committing a transaction, among other things.
These benchmark results can be the baseline, with the right two columns as eventual targets.
CalicoDB shouldn't ever be slower than this.

## TODO
1. Get everything code reviewed!
2. Need to implement repair (`Status Database::repair()`)
    + Run when a database cannot be opened due to corruption (not the same as recovery)
3. Support Windows (write a `Storage` implementation)
4. When we roll back a transaction, we shouldn't undo any vacuum operations that have occurred
   + Add a new WAL record type: a "meta" record that signals some operation and maybe a payload specific to the operation type
   + This record type can signal commit, vacuum start, or vacuum end, or even other things as needed
   + We should still roll vacuum operations forward during recovery
5. Reduce the overhead of the commit operation
   + Don't advance to a new WAL segment on commit
   + When committing, just write the WAL record, flush the tail buffer, and then call `fsync()` on the file handle
   + This will also make rolling the WAL faster since, depending on the commit frequency, we could end up with far fewer segment files

## Documentation
Check out Calico DB's [usage and design documents](doc).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.
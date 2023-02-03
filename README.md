# CalicoDB

> **Warning**: This library is not yet stable, nor is it code reviewed. 
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
+ Forward and reverse iteration using cursors
+ Crash protection using write-ahead logging
+ Variable-length keys and values (see [Caveats](#caveats))
+ Various parameters can be tuned (page size, cache size, etc.)

## Caveats
+ Only tested on 64-bit Ubuntu and OSX
+ Maximum key length is anywhere from 29 B to ~16 KiB, depending on the chosen page size
+ Maximum value length is roughly 4 GiB
+ Doesn't support concurrent transactions
+ Doesn't provide synchronization past support for concurrent cursors

## Documentation
Check out the [docs](doc/doc.md).

## Dependencies
The library itself depends on `@gabime/spdlog` and `@TartanLlama/expected`.
The tests depend on `@google/googletest`, and the benchmarks depend on `@google/benchmark`.
Dependencies are either downloaded using CMake's FetchContent API, or bundled with the source code.

## Performance
Calico DB is optimized for read-heavy workloads with intermittent batches of sequential writes.
Synchronization for multiple writers, or for simultaneous readers and writers, must be provided externally.
Performance benchmarks are provided in the [`benchmarks`](benchmarks) folder.

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Support Windows
4. Work on the documentation in [doc](doc)
5. Work on performance
    + B<sup>+</sup>-tree splitting algorithm could be modified to favor sequential writes less
6. Need some way to reduce the file size once many pages become unused
    + We need some way to collect freelist pages at the end of the file so that we can truncate
    + Look into SQLite's pointer maps

## Documentation
Check out Calico DB's [usage and design documents](doc).

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.



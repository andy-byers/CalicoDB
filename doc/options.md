# Calico DB Options
The behavior of a Calico DB instance can be modified by passing a set of options to the database constructor.
Available options include:

+ `page_size`: The size of a database page in bytes. 
Must be a power of two between 256 and 32768, inclusive.
+ `frame_count`: The number of page-sized frames to allow the block pool page cache.
+ `log_level`: Log level enumeration passed to `spdlog`. 
Determines the types of messages that get written to the log file.
+ `store`: See [store](#store).
+ `wal`: See [wal](#wal).

## Customization Points
The options object passed to the database during construction contains several customization points.
These come in the form of two pointers, `store` and `wal`, that allow custom objects to be injected.

### `wal`
A pointer to a WriteAheadLog interface object.
There is an implementation provided called BasicWriteAheadLog that handles reading and writing a persistent WAL consisting of multiple segment files.
If a `wal` pointer is not provided, the database will not support transactions.

### `store`
A pointer to a Storage interface object.
This time there are two implementations provided.
The first implementation, called DiskStorage, stores data on disk.
The second one, called HeapStorage, stores its data on the heap.

### Caveats
Note that once a database has been created with a given set of options, that database must always use those same options.
Exceptions to this rule are the block pool cache size and the log level.
Also, the database does not take ownership of the provided customization objects.
Rather, they are owned by the database creator and must be live for as long as the database is open.
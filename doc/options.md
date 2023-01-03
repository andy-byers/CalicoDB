# Calico DB Options
The behavior of a Calico DB instance can be modified by passing a set of options to the database constructor.
Available options include:

+ `page_size`: The size of a database page in bytes. 
Must be a power of two between 256 and 32768, inclusive.
+ `cache_size`: The number of page-sized frames to allow the block pool page cache.
+ `wal_limit`: Number of blocks before WAL segmentation (see [wal](./wal)).
+ `wal_prefix`: Location to store or find WAL segment files. (see [wal](./wal)).
+ `log_level`: Log level enumeration passed to `spdlog`. 
  Determines the types of messages that get written to the log file.
+ `store`: A pointer to a `Storage` interface object.
The default implementation stores data on disk and works on Unix platforms.

[//]: # (TODO: It would be nice to support Windows! I need help on this one...)

### Caveats
Note that once a database has been created with a given set of options, that database must always use those same options.
Exceptions to this rule are the page cache size and the log level.
Also, the database does not take ownership of the provided storage object.
Rather, it is owned by the database creator and must be live for as long as the database is open.
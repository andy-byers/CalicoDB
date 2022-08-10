# Buffer Pool
Accessing persistent storage is expensive, so Calico DB tries to avoid that where possible.
To that end, we reserve a portion of main memory for caching pages from disk.
Each database page, when read into main memory, lives in a data structure called a frame.
A frame stores the entire contents of a page along with some auxiliary data ("dirty" flag, reference count, etc.).
Both pages and frames are uniquely identified by their own indices.
A page's page ID gives the offset of the page in the database file, while a frame's frame ID is its index in the collection of available frames.

## Page
Note that the word "page" can also refer to the actual Calico DB Page object, which is returned by the buffer pool when a page is requested.
Page objects can be either read-only or read-write, and are reference counted.
Typically, pages are acquired as read-only and promoted to read-write when an update is about to be made.
This subverts the overhead of keeping a "before" copy of the page for recovery (see [wal.md](./wal.md) for more details).
It also allows us to obtain multiple read-only references to a single database page for use in concurrent queries.

## Pager
The pager object is in charge of "pinning" database pages to buffer pool frames.
When a page is pinned, it is read from the database file into an available frame.
Once the page is no longer needed, it can be "unpinned".
This involves writing it back to the database file if it is dirty.
It is the buffer pool's job, however, to ensure that if a dirty page is unpinned, its contents are already in the WAL.
This is achieved by checking the page LSN against the WAL flushed LSN.
If the WAL flushed LSN is greater than or equal to the page LSN, then the page is safe to flush.

## Page Cache
The page cache provides an in-memory mapping between frame IDs and page IDs.
Its heuristics govern which pages are kept pinned in memory and which are written back to disk and their frames reused.
The current implementation uses a variation of the 2Q replacement policy.
We keep two caches, one is an LRU cache, and the other is a FIFO cache.
Let the LRU cache be called the "hot" cache, and the FIFO cache be called the "warm" cache.
Neither cache enforces a capacity, so entries must be evicted manually.
Each entry consists of a page ID, a frame ID, and a dirty list pointer (see [Dirty List](#dirty-list) below), and is keyed by the page ID.
When an entry is first inserted into the cache, it starts out "warm".
If it is referenced again, while it is still in the warm cache, it gets transfer to the hot cache.
When an entry is evicted, the warm cache is looked at before the hot cache.
This causes often-used entries to remain in the cache, and seldom-used entries to be reused more often.
Check out https://arpitbhayani.me/blogs/2q-cache for a better description of 2Q.
For reference, we are using something like the simplified version from this article.

[//]: # (TODO: May upgrade to the full 2Q algorithm, which uses another queue, if deemed necessary for performance. However, it seems to work pretty well as-is.)

## Dirty List
The buffer pool cache can contain anywhere from 8 to 8192 entries.
It can be pretty expensive to repeatedly search this many entries when looking for a frame to reuse.
To help mitigate this cost, we keep a dirty list.
The dirty list is a linked list containing the page IDs of dirty database pages.
As mentioned above in [Page Cache](#page-cache), each page cache entry stores a pointer to a dirty list entry.
This allows one to find a dirty list entry in constant time, given its associated page cache entry.
Similarly, we can use the page ID contained in a dirty list entry to fetch the associated page cache entry.

[//]: # (TODO: The dirty list is not implemented. Not entirely sure if we need it, but I believe the above design would work. I'll be refactoring the page cache so that this design works, as it should be better in other ways too!)
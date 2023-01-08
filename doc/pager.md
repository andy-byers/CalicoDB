# Pager
Accessing persistent storage is expensive, so Calico DB tries to avoid that where possible.
To that end, we reserve a portion of main memory for caching pages from disk.
Each database page, when read into main memory, lives in a data structure called a frame.
A frame stores the entire contents of a page along with some auxiliary information (reference count, "is writable" enabled, etc.).
Both pages and frames are uniquely identified by their own indices.
A page's index, called its page ID, gives the offset of the page in the database file, while a frame's frame ID is its index in the set of frames.
Note that page IDs, along with several other types of identifiers, start at 1, with 0 being reserved as a "null" value.

## Page
The word "page" can refer to either a fixed-size chunk of the database file, or the actual `Page` object returned by the pager component.
The actual `Page` object is just a handle of sorts, used to access the contents of a frame and help with reference counting.
Page objects can be either read-only or read-write.
Typically, pages are acquired as read-only and promoted to read-write when an update is about to be made.
This subverts the need to keep a "before" copy of the page for recovery (see [wal.md](./wal.md) for more details).
It also allows us to obtain multiple read-only references to a single database page for use in concurrent queries.

## Framer
The framer object is in charge of "pinning" database pages to frames.
When a page is pinned, it is read from the database file into an available frame.
Once the page is no longer needed, it can be "unpinned", given that it is not dirty.
If the page is dirty, it must first be written back to the database file, and for that to be possible, its contents must already be in the WAL.
It is the caller's job to ensure that if a dirty page needs to be written back, its contents are already in the WAL.
This is achieved by checking the page LSN against the WAL flushed LSN.
If the WAL flushed LSN is greater than or equal to the page LSN, then the page is safe to write.

## Page Cache
The page cache provides an in-memory mapping between frame IDs and page IDs.
Its heuristics govern which pages are kept pinned in memory and which are written back to disk and their frames reused.
The current implementation uses a variation of the 2Q replacement policy.
We keep two caches, one is a least-recently used (LRU) cache, and the other is a first-in, first-out (FIFO) cache.
Let the LRU cache be called the "hot" cache, and the FIFO cache be called the "warm" cache.
Each entry consists of a page ID, a frame ID, and a dirty list pointer (see [Dirty List](#dirty-list) below), and is keyed by the page ID.
When an entry is first inserted into the cache, it starts out "warm".
If it is referenced again, while it is still in the warm cache, it gets transfered to the hot cache.
When an entry is evicted, the warm cache is searched before the hot cache.
This causes often-used entries to remain in the cache, and seldom-used entries to be reused more often.
Check out https://arpitbhayani.me/blogs/2q-cache for a better description of 2Q.
For reference, we are using something like the simplified version from this article.
Also note that neither cache enforces a capacity, so entries must be evicted manually.

## Dirty List
The pager cache can contain anywhere from 8 to 8 K entries.
It can be pretty expensive to repeatedly search this many entries when attempting to flush the cache, which happens during the abort routine.
In order to make abort-heavy workloads faster, we keep a dirty list which lets us iterate over exactly the pages that are dirty.
The dirty list is a linked list containing the page IDs of all dirty database pages.
As mentioned above in [Page Cache](#page-cache), each page cache entry stores a pointer to a dirty list entry.
This allows one to find a dirty list entry in constant time, given its associated page cache entry.
Similarly, we can use the page ID contained in a dirty list entry to fetch the associated page cache entry.
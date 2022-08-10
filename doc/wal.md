# Write-Ahead Log (WAL)
Calico DB uses write-ahead logging to protect transactions.
Each update to a database page must first be written to the WAL, and WAL segments cannot be removed until all of their corresponding database pages have been flushed to disk.
A physical logging scheme is used, described in [Logging Scheme](#logging-scheme).
This incurs additional overhead compared to higher-level logging schemes, but also protects against crashes during tree rebalancing and other routines.
Calico DB only supports a single running transaction, so all updates go to the same WAL segment.
When the transaction is committed, a commit record is written at the end of the current segment, and a new segment is generated.
Updates made on the new transaction will be written to the new WAL segment.
The old segment will be deleted once all updates that it contains have been flushed to the database file.
A new WAL segment is also generated once the old one has reached a user-provided threshold.

## Background Writer
Working with WAL records (both constructing them and writing them to disk) is expensive.
Luckily, much of this work does not depend on other database components and can be done in the background.
The background writer is used to create WAL records out of page update information and write them to disk.
It keeps track of the log sequence number (LSN) of the last record it has written to disk, so that the buffer pool component knows which pages are safe to write back and reuse.

## Logging Scheme
The WAL uses physical logging to protect pages during a transaction.
What follows is a short description of the lifecycle of a **writable** database page during a transaction.
Assuming that the requested page $P$ is not already in the buffer pool cache, we first read $P$ into a vacant frame.
Thus far, $P$ has not been modified, so we write out a WAL record containing its entire "before" contents, with its page LSN updated to reflect the new WAL record.
Note that the record will also contain the page's previous LSN, so we can always revert the page to its state before this operation.
Now we are free to modify $P$.
Each time we do so, $P$ will record a (offset, size) pair describing the modified region.
Once $P$ is released, all of these pairs are consolidated such that there are no overlaps.
Since each WAL record references only a single page, consolidation of these pairs has the benefit of bounding the maximum record size.
Finally, the "after" contents of the modified regions are bundled into another WAL record and written to disk.
This procedure has the downside of causing extra data to be written to the WAL when a given page is only updated a few times.
However, for pages that are modified frequently, the reduction in WAL size justifies use of this scheme.

[//]: # (TODO: Logging scheme also can cause extra full-page records to be written due to buffer pool frame stealing (a dirty page gets written back, reused, then made dirty again, in the same transaction.)
[//]: # (TODO: We can prevent extra full-page records by keeping track of which pages we have records for during each transaction and refusing to make them twice for a given page.)

# Write-Ahead Log (WAL)
Calico DB uses write-ahead logging to protect transactions.
Each update to a database page must first be written to the WAL, and WAL segments cannot be removed until all of their corresponding database pages have been flushed to disk.
A physical logging scheme is used, so the contents of each updated region (before and after the update) are written to the WAL.
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
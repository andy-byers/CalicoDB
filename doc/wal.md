# Write-Ahead Log (WAL)
Calico DB uses write-ahead logging to provide atomicity to database transactions.

### Log Segments
Calico DB only supports a single running transaction, so all updates go to the same WAL segment.
When the transaction is committed, a commit record is written at the end of the current segment, and a new segment is generated.
Updates made on the new transaction will be written to the new WAL segment.
The old segment will be deleted once all updates that it contains have been flushed to the database file.
A new WAL segment is also generated once the old one has reached a user-provided threshold.

### Background Writer
Working with WAL records (both constructing them and writing them to disk) is expensive.
Luckily, much of this work does not depend on other database components and can be done in the background.
The background writer is used to create WAL records out of page update information and write them to disk.
It keeps track of the log sequence number (LSN) of the last record it has written to disk, so that the pager component knows which pages are safe to write back and reuse.

### Logging Scheme
The WAL uses physical logging to protect pages during a transaction.
What follows is a short description of the lifecycle of a **writable** database page during a transaction.
Assuming that the requested page $P$ is not already in the pager cache, we first read $P$ into a vacant frame.
Thus far, $P$ has not been modified, so we write out a WAL record containing its entire "before" contents, called a full image record (see [Full Images](#full-images)).
Writing out this WAL record represents a promise that the database page will actually be modified: the tree component should never acquire a writable page unless it is actually going to write to it.
Now that $P$ has its contents saved in the WAL, we are free to modify it.
Each time we do so, $P$ will record a (offset, size) pair describing the modified region.
Once $P$ is released, all of these pairs are consolidated such that there are no overlaps.
Since each WAL record references only a single page, consolidation of these pairs has the benefit of bounding the maximum record size.
Finally, the "after" contents of the modified regions are bundled into another WAL record and written to disk (see [Delta Records](#delta-records)).
This procedure has the downside of causing extra data to be written to the WAL when a given page is only updated a few times.
However, for pages that are modified frequently, the reduction in WAL size justifies use of this scheme.

### Record Types
The WAL deals with three types of records: full images, delta records, and commit records.

#### Full Images
A full-page image is generated when a clean writable page is acquired from the pager.
It contains the entire page contents, and can be used to undo many modifications made during a transaction.
All that is needed for a full image is an immutable slice of the page data.
The page LSN is not updated when generating this type of record, since the page was not actually updated.

[//]: # (TODO: I don't see a problem with this, but who knows. We may need to update the page LSN with this type of record.
               If so, we can let the changes be part of the next delta record.)

#### Delta Records
A delta record is generated when a page is released back to the pager after being modified.
Delta records contain only the exact changes made to a page.

#### Commit Records
A commit record is emitted when a transaction commits.
It contains no actual information (other than its LSN) and serves as a sentinel to mark that a transaction has succeeded.

### Recovery
Together, records of these three record types can be used to undo and redo modifications made to database pages.
We split recovery up into two phases: redo and undo.

#### Redo Phase
During the redo phase, we roll the WAL forward and apply updates that aren't already reflected in the database (using the database flushed LSN for reference).
If we find a commit record at the end of the log, we are done, and the database is up-to-date.
Otherwise, we must enter the second phase of recovery: undo.

### Undo Phase
In the undo phase, we roll back the most recent (incomplete) transaction, that is, we roll the log backward from the end until the most recent commit.
Now the database is consistent with its state at the last successful commit.

### Reentrancy
Both of these phases involve numerous disk accesses, and thus have many opportunities for failure.
The WAL object must guarantee that recovery as-a-whole is reentrant.
We should be able to perform both recovery phases multiple times without corrupting the database.
It should be noted that this routine does not protect from database corruption in most cases, it just provides atomicity and added durability to transactions.






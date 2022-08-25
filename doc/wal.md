# Write-Ahead Log (WAL)
Calico DB uses write-ahead logging to provide atomicity to database transactions.

[//]: # (TODO: Removing this customization point. It's not worth it in my opinion. We need more control over synchronization...)

## WAL Interface
The WAL interface is represented by an abstract class, called `WriteAheadLog`.
If custom behavior is desired, users can inherit from this class and pass an instance to the database to use.
Otherwise, passing in `nullptr` will cause the database to use the default implementation.
Classes that implement the WAL interface must abide by certain rules for everything to work correctly.
The pre-/post-conditions required for proper operation are outlined in [wal.h](../src/wal/wal.h).
Here, we provide a higher-level overview of how the WAL component fits into the overall database design.

First, the WAL object is not required to perform logging in the main thread.
Being relatively expensive, the default implementation chooses to do this in a background thread.
All that is required is that when the `start()` method returns, the WAL is ready to be written, and when the `stop()` method returns, the WAL cannot be written anymore.
The database will only call `stop()` when it needs to read WAL records (during abort or recovery), so this shouldn't happen very often (compared to other operations like logging records or committing).

## Default Implementation
The default WAL implementation, called `BasicWriteAheadLog`, manages a series of segment files, each split up into multiple blocks.
Each block contains one or more WAL records.

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
It keeps track of the log sequence number (LSN) of the last record it has written to disk, so that the block pool component knows which pages are safe to write back and reuse.

### Logging Scheme
The WAL uses physical logging to protect pages during a transaction.
What follows is a short description of the lifecycle of a **writable** database page during a transaction.
Assuming that the requested page $P$ is not already in the block pool cache, we first read $P$ into a vacant frame.
Thus far, $P$ has not been modified, so we write out a WAL record containing its entire "before" contents, called a full image record (see [Full Images](#full-images)).
Writing out this WAL record represents a promise that the database page will actually be modified: the tree component should never acquire a writable page unless it is actually going to write to it.
Now that $P$ has its contents saved in the WAL, we are free to modify it.
Each time we do so, $P$ will record a (offset, size) pair describing the modified region.
Once $P$ is released, all of these pairs are consolidated such that there are no overlaps.
Since each WAL record references only a single page, consolidation of these pairs has the benefit of bounding the maximum record size.
Finally, the "after" contents of the modified regions are bundled into another WAL record and written to disk (see [Delta Records](#delta-records)).
This procedure has the downside of causing extra data to be written to the WAL when a given page is only updated a few times.
However, for pages that are modified frequently, the reduction in WAL size justifies use of this scheme.

[//]: # (TODO: Logging scheme also can cause extra full-page records to be written due to block pool frame stealing (a dirty page gets written back, reused, then made dirty again, in the same transaction.)
[//]: # (TODO: We can prevent extra full-page records by keeping track of which pages we have records for during each transaction and refusing to make them twice for a given page.)

## Customization
As mentioned in [options.md](#./options.md), it is possible to pass a pointer to a custom WAL object to the database constructor.
In this case, the database will interact with the passed object through the WriteAheadLog interface.
The custom WAL object should keep track of the log sequence number (LSN) of the last record it has written to disk, called the flushed LSN, so that the block pool can refrain from writing out non-guaranteed pages.
The flushed LSN value provided does not need to be exact, however, it must be equal to or lower than the actual flushed LSN.
This allows writing to be done in the background.

### Record Types
The custom WAL object must handle three types of records: full images, delta records, and commit records.

#### Full Images
A full-page image is generated when a clean writable page is acquired from the pager.
It contains the entire page contents, and can be used to undo many modifications made during a transaction.
All that is needed for a full image is an immutable slice of the page data.
The page LSN is not updated when generating this type of record, since the page was not actually updated.

[//]: # (TODO: I don't see a problem with this, but who knows. We may need to update the page LSN with this type of record.
               If so, we can let the changes be part of the next delta record.)

#### Delta Records
A delta record is generated when a page is released back to the block pool after being modified.
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
The custom WAL object must guarantee that recovery as-a-whole is reentrant.
We should be able to perform both recovery phases multiple times without corrupting the database.
It should be noted that this routine does not protect from database corruption in most cases, it just provides atomicity and added durability to transactions.






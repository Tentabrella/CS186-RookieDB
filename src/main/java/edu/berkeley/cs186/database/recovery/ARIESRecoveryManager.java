package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // write commit log and flush record
        long lsn = appendLogHelper(new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        logManager.flushToLSN(lsn);
        // change status to commit
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // write abort log no need to flush
        long lsn = appendLogHelper(new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        // change status to abort
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        return lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // perform rollback if abort
        if (transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING) {
            long lastLSN = transactionTable.get(transNum).lastLSN;
            Optional<Long> prevLSN = Optional.of(lastLSN);
            LogRecord logRecord;
            do {
                logRecord = logManager.fetchLogRecord(prevLSN.get());
                prevLSN = logRecord.getPrevLSN();
            } while (prevLSN.isPresent());
            rollbackToLSN(transNum, logRecord.getLSN());
        }
        // write complete log no need to flush
        long lsn = appendLogHelper(new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        // change status to end
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        // remove from transaction table
        transactionTable.remove(transNum);
        return lsn;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Emit the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        while (currentLSN > LSN) {
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                // generate undo, append to log
                LogRecord undoRecord = logRecord.undo(transactionTable.get(transNum).lastLSN);
                appendLogHelper(undoRecord);
                undoRecord.redo(this, this.diskSpaceManager, this.bufferManager);
            }
            // theoretically above will jump to non clr, so no need to call undoNext, just prev
            currentLSN = logRecord.getPrevLSN().orElse(-1L);
        }
    }

    /**
     * Append the record of txn into log and update transaction table
     * @param record One record of some transaction
     * @return LSN of this record
     */
    private long appendLogHelper(LogRecord record) {
        TransactionTableEntry transactionTableEntry;
        if (record.getTransNum().isPresent()) {
            transactionTableEntry = this.transactionTable.get(record.getTransNum().get());
            transactionTableEntry.lastLSN = this.logManager.appendToLog(record);
            return transactionTableEntry.lastLSN;
        } else {
            throw new UnsupportedOperationException("Record to append need to have a txnNumber");
        }
    }


    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // wal append log
        long lsn = appendLogHelper(new UpdatePageLogRecord(transNum, pageNum, this.transactionTable.get(transNum).lastLSN,
                pageOffset, before, after));
        // update dpt if null
        this.dirtyPageTable.putIfAbsent(pageNum, lsn);
        return lsn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // roll back to lsn
        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // write dpt and transaction table
        List<EndCheckpointLogRecord> endCheckpointLogRecords = loadEndChkpt();
        for (int i = 0; i < endCheckpointLogRecords.size() - 1; i++) {
            logManager.appendToLog(endCheckpointLogRecords.get(i));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        if (endCheckpointLogRecords.size() > 0) {
            endRecord = endCheckpointLogRecords.get(endCheckpointLogRecords.size() - 1);
        }
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Creating a list of EndCheckPointRecord using the dpt and txn table at that moment
     * @return a list of EndCheckPointRecords
     */
    private List<EndCheckpointLogRecord> loadEndChkpt() {
        Iterator<Long> dptIter = this.dirtyPageTable.keySet().iterator();
        Iterator<Long> txnIter = this.transactionTable.keySet().iterator();
        List<EndCheckpointLogRecord> endCheckpointLogRecords = new ArrayList<>();

        int remainDptRecord = this.dirtyPageTable.size();
        int remainTxnRecord = this.transactionTable.size();
        List<int[]> loadPairs = new ArrayList<>();

        while (remainDptRecord > 0 || remainTxnRecord > 0) {
            int numDptRecord = 0;
            int numTxnTableRecord = 0;
            while (EndCheckpointLogRecord.fitsInOneRecord(numDptRecord, numTxnTableRecord) &&
                    (numDptRecord < remainDptRecord || numTxnTableRecord < remainTxnRecord)) {
                if (numDptRecord < remainDptRecord) {
                    numDptRecord++;
                } else {
                    numTxnTableRecord++;
                }
            }
            if (!EndCheckpointLogRecord.fitsInOneRecord(numDptRecord, numTxnTableRecord)) {
                if (numTxnTableRecord > 0) {
                    numTxnTableRecord--;
                } else if (numDptRecord > 0) {
                    numDptRecord--;
                }
            }
            loadPairs.add(new int[]{numDptRecord, numTxnTableRecord});
            remainDptRecord -= numDptRecord;
            remainTxnRecord -= numTxnTableRecord;
        }

        for (int[] loadPair : loadPairs) {
            Map<Long, Long> dirtyPageTable = new HashMap<>();
            Map<Long, Pair<Transaction.Status, Long>> transactionTable = new HashMap<>();
            for (int i = 0; i < loadPair[0]; i++) {
                Long pageNum = dptIter.next();
                dirtyPageTable.put(pageNum, this.dirtyPageTable.get(pageNum));
            }
            for (int i = 0; i < loadPair[1]; i++) {
                Long txnNum = txnIter.next();
                TransactionTableEntry transactionTableEntry = this.transactionTable.get(txnNum);
                transactionTable.put(txnNum, new Pair<>(transactionTableEntry.transaction.getStatus(),
                        transactionTableEntry.lastLSN));
            }
            endCheckpointLogRecords.add(new EndCheckpointLogRecord(dirtyPageTable, transactionTable));
        }
        return endCheckpointLogRecords;
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related, update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records are processed, cleanup and end transactions that are in
     * the COMMITING state, and move all transactions in the RUNNING state to
     * RECOVERY_ABORTING/emit an abort record.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // scan log
        Iterator<LogRecord> logRecordIterator = this.logManager.scanFrom(LSN);
        while (logRecordIterator.hasNext()) {
            LogRecord logRecord = logRecordIterator.next();
            // If the log record is for a transaction operation
            Long transNum = -1L;
            if (logRecord.getTransNum().isPresent()) {
                 transNum = logRecord.getTransNum().get();
                if (!this.transactionTable.containsKey(transNum)) {
                    this.startTransaction(this.newTransaction.apply(transNum));
                }
                this.transactionTable.get(transNum).lastLSN = logRecord.getLSN();
            }
            // If the log record is page-related, update the dpt
            if (logRecord.getType() == LogType.UPDATE_PAGE || logRecord.getType() == LogType.UNDO_UPDATE_PAGE) {
                this.dirtyPageTable.putIfAbsent(logRecord.getPageNum().get(), logRecord.getLSN());
            }
            if (logRecord.getType() == LogType.FREE_PAGE || logRecord.getType() == LogType.UNDO_ALLOC_PAGE) {
                this.dirtyPageTable.remove(logRecord.getPageNum().get());
            }
            // If the log record is for a change in transaction status
            if (logRecord.getType() == LogType.END_TRANSACTION) {
                this.transactionTable.get(transNum).transaction.cleanup();
                this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                this.transactionTable.remove(transNum); // you need to remove yourself since this is recoveryTransaction which will not call end()!
                endedTransactions.add(transNum);
            }
            if (logRecord.getType() == LogType.COMMIT_TRANSACTION) {
                this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
            }
            if (logRecord.getType() == LogType.ABORT_TRANSACTION) {
                this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            // If the log record is an end_checkpoint record
            if (logRecord.getType() == LogType.END_CHECKPOINT) {
                Map<Long, Long> dpt = logRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> txnTable = logRecord.getTransactionTable();
                // Copy all entries of checkpoint DPT since dirty page of checkpoint can have an earlier recLSN
                for (Long pageNum : dpt.keySet()) {
                    this.dirtyPageTable.put(pageNum, dpt.get(pageNum));
                }
                // Dealing with transaction table (add newer information by skip, update, add)
                for (Long txnNum : txnTable.keySet()) {
                    // skip if status is complete since this will always be newer information
                    if (endedTransactions.contains(txnNum)) {
                        continue;
                    }
                    // add if not exist before
                    this.transactionTable.putIfAbsent(txnNum, new TransactionTableEntry(this.newTransaction.apply(txnNum)));
                    // update status to the newer status
                    Transaction.Status chkptStatus = txnTable.get(txnNum).getFirst();
                    Long chkptLastLSN = txnTable.get(txnNum).getSecond();
                    if (chkptStatus.compareTo(this.transactionTable.get(txnNum).transaction.getStatus()) > 0) {
                        this.transactionTable.get(txnNum).transaction.setStatus(
                                chkptStatus == Transaction.Status.ABORTING ? Transaction.Status.RECOVERY_ABORTING : chkptStatus);
                    }
                    if (chkptLastLSN > this.transactionTable.get(txnNum).lastLSN) {
                        this.transactionTable.get(txnNum).lastLSN = chkptLastLSN;
                    }
                }
            }
        }
        // after scanning, end committing (or you won't get chance to end this later), abort running
        // no need to worry if end committing will influence commit durability. you will redo all process like
        // what a transaction actually does(you can see we already do this in rollback: adjusting something like txn).
        for (Long transNum : transactionTable.keySet()) {
            TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
            if (transactionTableEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                transactionTableEntry.transaction.cleanup();
                end(transNum);
            } else if (transactionTableEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                transactionTableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                appendLogHelper(new AbortTransactionLogRecord(transNum, transactionTableEntry.lastLSN));
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a partition (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        Optional<Long> optionalFirstLSN = this.dirtyPageTable.values().stream().min(Long::compare);
        // empty dpt
        if (!optionalFirstLSN.isPresent()) {
            return;
        }
        long firstLSN = optionalFirstLSN.get();
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(firstLSN);
        // scanning from firstLSN
        while (logRecordIterator.hasNext()) {
            LogRecord logRecord = logRecordIterator.next();
            // not redoable
            if (!logRecord.isRedoable()) {
                continue;
            }
            // about a partition
            if (isPartitionType(logRecord.getType())) {
                logRecord.redo(this, this.diskSpaceManager, this.bufferManager);
            }
            // allocates a page
            if (isAllocatePageType(logRecord.getType())) {
                logRecord.redo(this, this.diskSpaceManager, this.bufferManager);
            }
            // modifies a page
            if (isModifyPageType(logRecord.getType())) {
                Long pageNum = logRecord.getPageNum().get();
                if (this.dirtyPageTable.containsKey(pageNum) && this.dirtyPageTable.get(pageNum) <= logRecord.getLSN()) {
                    Page page = this.bufferManager.fetchPage(new DummyLockContext(), pageNum);
                    try {
                        if (page.getPageLSN() < logRecord.getLSN()) {
                            logRecord.redo(this, this.diskSpaceManager, this.bufferManager);
                        }
                    } finally {
                        page.unpin();
                    }
                }
            }
        }
    }

    private boolean isModifyPageType(LogType type) {
        Set<LogType> types = new HashSet<>(Arrays.asList(LogType.UPDATE_PAGE, LogType.UNDO_UPDATE_PAGE,
                LogType.FREE_PAGE, LogType.UNDO_ALLOC_PAGE));
        return types.contains(type);
    }

    private boolean isAllocatePageType(LogType type) {
        Set<LogType> types = new HashSet<>(Arrays.asList(LogType.ALLOC_PAGE, LogType.UNDO_FREE_PAGE));
        return types.contains(type);
    }

    private boolean isPartitionType(LogType type) {
        Set<LogType> types = new HashSet<>(Arrays.asList(LogType.ALLOC_PART, LogType.UNDO_ALLOC_PART,
                LogType.FREE_PART, LogType.UNDO_FREE_PART));
        return types.contains(type);
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and emit the appropriate CLR
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if not available) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        PriorityQueue<Long> toUndoLSNs = new PriorityQueue<>(Collections.reverseOrder());
        for (TransactionTableEntry transactionTableEntry : transactionTable.values()) {
            toUndoLSNs.add(transactionTableEntry.lastLSN);
        }
        while (!toUndoLSNs.isEmpty() && toUndoLSNs.peek() != 0L) {
            Long undoLSN = toUndoLSNs.poll();
            undoLSN = this.logManager.fetchLogRecord(undoLSN).getUndoNextLSN().orElse(undoLSN);

            LogRecord undoRecord = this.logManager.fetchLogRecord(undoLSN);
            Long undoTransNum = undoRecord.getTransNum().get();
            TransactionTableEntry undoTransactionTableEntry = this.transactionTable.get(undoTransNum);
            // if undoable, undo
            if (undoRecord.isUndoable()) {
                LogRecord clrRecord = undoRecord.undo(undoTransactionTableEntry.lastLSN);
                appendLogHelper(clrRecord);
                clrRecord.redo(this, this.diskSpaceManager, this.bufferManager);
            }
            // replace the entry in the set should be replaced with a new one, using the undoNextLSN
            Long undoNextLSN = undoRecord.getUndoNextLSN().orElse(undoRecord.getPrevLSN().orElse(0L));
            toUndoLSNs.add(undoNextLSN);
            // if the new LSN is 0, end the transaction and remove it from the queue and transaction table
            if (undoNextLSN == 0L) {
                undoTransactionTableEntry.transaction.cleanup();
                end(undoTransNum);
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}

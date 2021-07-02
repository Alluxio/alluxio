/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal.ufs;

import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JournalClosedException;
import alluxio.exception.JournalClosedException.IOJournalClosedException;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.master.journal.JournalWriter;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.OpenOptions;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for writing journal edit log entries from the primary master. It marks the current log
 * complete (so that it is visible to the secondary masters) when the current log is large enough.
 *
 * When a new journal writer is created, it also marks the current log complete if there is one.
 *
 * A journal garbage collector thread is created when the writer is created, and is stopped when the
 * writer is closed.
 */
@ThreadSafe
final class UfsJournalLogWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalLogWriter.class);

  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;

  /** The maximum size in bytes of a log file. */
  private final long mMaxLogSize;

  /** The next sequence number to use. */
  private long mNextSequenceNumber;
  /** When mRotateForNextWrite is set, mJournalOutputStream must be closed before the next write. */
  private boolean mRotateLogForNextWrite;
  /**
   * The output stream to write the journal log entries.
   * Initially this field is null.
   * Also set this field to null when an {@link IOException} is caught.
   */
  private JournalOutputStream mJournalOutputStream;
  /** The garbage collector. */
  private UfsJournalGarbageCollector mGarbageCollector;
  /** Whether the journal log writer is closed. */
  private boolean mClosed;

  /**
   * Set mNeedsRecovery to true when an IOException is thrown when trying to write journal entries.
   * Clear this flag when {@link #maybeRecoverFromUfsFailures()} successfully recovers.
   */
  private boolean mNeedsRecovery = false;
  /**
   * Journal entries that have been written successfully to the underlying
   * {@link DataOutputStream}, but have not been flushed. Should a failure occur
   * before flush, {@code UfsJournalLogWriter} is able to retry writing the
   * journal entries.
   */
  private Queue<JournalEntry> mEntriesToFlush;

  /**
   * Creates a new instance of {@link UfsJournalLogWriter}.
   *
   * @param journal the handle to the journal
   * @param nextSequenceNumber the sequence number to begin writing at
   */
  UfsJournalLogWriter(UfsJournal journal, long nextSequenceNumber) throws IOException {
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = mJournal.getUfs();
    mNextSequenceNumber = nextSequenceNumber;
    mMaxLogSize = ServerConfiguration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);

    mRotateLogForNextWrite = true;
    UfsJournalFile currentLog = UfsJournalSnapshot.getCurrentLog(mJournal);
    if (currentLog != null) {
      mJournalOutputStream = new JournalOutputStream(currentLog, ByteStreams.nullOutputStream());
    }
    mGarbageCollector = new UfsJournalGarbageCollector(mJournal);
    mEntriesToFlush = new ArrayDeque<>();
  }

  public synchronized void write(JournalEntry entry) throws IOException, JournalClosedException {
    checkIsWritable();
    try {
      maybeRecoverFromUfsFailures();
      maybeRotateLog();
    } catch (IOJournalClosedException e) {
      throw e.toJournalClosedException();
    }

    try {
      JournalEntry entryToWrite =
          entry.toBuilder().setSequenceNumber(mNextSequenceNumber).build();
      entryToWrite.writeDelimitedTo(mJournalOutputStream);
      LOG.debug("Adding journal entry (seq={}) to retryList with {} entries. currentLog: {}",
          entryToWrite.getSequenceNumber(), mEntriesToFlush.size(), currentLogName());
      mEntriesToFlush.add(entryToWrite);
      mNextSequenceNumber++;
    } catch (IOJournalClosedException e) {
      throw e.toJournalClosedException();
    } catch (IOException e) {
      // Set mNeedsRecovery to true so that {@code maybeRecoverFromUfsFailures}
      // can know a UFS failure has occurred.
      mNeedsRecovery = true;
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_FAILURE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
              mJournalOutputStream.currentLog(), e.getMessage()), e);
    }
  }

  /**
   * Core logic of UFS journal recovery from UFS failures.
   *
   * If Alluxio stores its journals in UFS, then Alluxio needs to handle UFS failures.
   * When UFS is dead, there is nothing Alluxio can do because Alluxio relies on UFS to
   * persist journal entries. Consequently any metadata operation will block because Alluxio
   * cannot flush their journal entries.
   * Once UFS comes back online, Alluxio needs to perform the following operations:
   * 1. Find out the sequence number of the last persisted journal entry, say X. Then the first
   *    non-persisted entry has sequence number Y = X + 1.
   * 2. Check whether there is any missing journal entry between Y (inclusive) and the oldest
   *    entry in mEntriesToFlush, say Z. If Z > Y, then it means journal entries in [Y, Z) are
   *    missing, and Alluxio cannot recover. Otherwise, for each journal entry in
   *    {@link #mEntriesToFlush}, if its sequence number is larger than or equal to Y, retry
   *    writing it to UFS by calling the {@code UfsJournalLogWriter#write} method.
   */
  private void maybeRecoverFromUfsFailures() throws IOException, JournalClosedException {
    checkIsWritable();
    if (!mNeedsRecovery) {
      return;
    }

    try (Timer.Context ctx = MetricsSystem
        .timer(MetricKey.MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER.getName()).time()) {
      long lastPersistSeq = recoverLastPersistedJournalEntry();
      if (lastPersistSeq == -1) {
        throw new RuntimeException(
            "Cannot find any journal entry to recover. location: " + mJournal.getLocation());
      }

      createNewLogFile(lastPersistSeq + 1);
      if (!mEntriesToFlush.isEmpty()) {
        JournalEntry firstEntryToFlush = mEntriesToFlush.peek();
        if (firstEntryToFlush.getSequenceNumber() > lastPersistSeq + 1) {
          throw new RuntimeException(ExceptionMessage.JOURNAL_ENTRY_MISSING.getMessageWithUrl(
              RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
              lastPersistSeq + 1, firstEntryToFlush.getSequenceNumber()));
        }
        long retryEndSeq = lastPersistSeq;
        LOG.info("Retry writing unwritten journal entries from seq {} to currentLog {}",
            lastPersistSeq + 1, currentLogName());
        for (JournalEntry entry : mEntriesToFlush) {
          if (entry.getSequenceNumber() > lastPersistSeq) {
            try {
              entry.toBuilder().build().writeDelimitedTo(mJournalOutputStream);
              retryEndSeq = entry.getSequenceNumber();
            } catch (IOJournalClosedException e) {
              throw e.toJournalClosedException();
            } catch (IOException e) {
              throw new IOException(ExceptionMessage.JOURNAL_WRITE_FAILURE
                  .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
                      mJournalOutputStream.currentLog(), e.getMessage()), e);
            }
          }
        }
        LOG.info("Finished writing unwritten journal entries from {} to {}. currentLog: {}",
            lastPersistSeq + 1, retryEndSeq, currentLogName());
        if (retryEndSeq != mNextSequenceNumber - 1) {
          throw new RuntimeException("Failed to recover all entries to flush, expecting " + (
              mNextSequenceNumber - 1) + " but only found entry " + retryEndSeq + " currentLog: "
              + currentLogName());
        }
      }
    }
    mNeedsRecovery = false;
  }

  /**
   * Examine the UFS to determine the most recent journal entry, and return its sequence number.
   *
   * 1. Locate the most recent incomplete journal file, i.e. journal file that starts with
   *    a valid sequence number S (hex), and ends with 0x7fffffffffffffff. The journal file
   *    name encodes this information, i.e. S-0x7fffffffffffffff.
   * 2. Sequentially scan the incomplete journal file, and identify the last journal
   *    entry that has been persisted in UFS. Suppose it is X.
   * 3. Rename the incomplete journal file to S-<X+1>. Future journal writes will write to
   *    a new file named <X+1>-0x7fffffffffffffff.
   * 4. If the incomplete journal does not exist or no entry can be found in the incomplete
   *    journal, check the last complete journal file for the last persisted journal entry.
   *
   * @return sequence number of the last persisted journal entry, or -1 if no entry can be found
   */
  private long recoverLastPersistedJournalEntry() throws IOException {
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    long lastPersistSeq = -1;
    UfsJournalFile currentLog = snapshot.getCurrentLog(mJournal);
    if (currentLog != null) {
      LOG.info("Recovering from previous UFS journal write failure."
          + " Scanning for the last persisted journal entry. currentLog: " + currentLog.toString());
      try (JournalEntryStreamReader reader =
          new JournalEntryStreamReader(mUfs.open(currentLog.getLocation().toString(),
              OpenOptions.defaults().setRecoverFailedOpen(true)))) {
        JournalEntry entry;
        while ((entry = reader.readEntry()) != null) {
          if (entry.getSequenceNumber() > lastPersistSeq) {
            lastPersistSeq = entry.getSequenceNumber();
          }
        }
      } catch (IOException e) {
        throw e;
      }
      if (lastPersistSeq != -1) { // If the current log is an empty file, do not complete with SN: 0
        completeLog(currentLog, lastPersistSeq + 1);
      }
    }
    // Search for and scan the latest COMPLETE journal and find out the sequence number of the
    // last persisted journal entry, in case no entry has been found in the INCOMPLETE journal.
    if (lastPersistSeq < 0) {
      // Re-evaluate snapshot because the incomplete journal will be destroyed if
      // it does not contain any valid entry.
      snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
      // journalFiles[journalFiles.size()-1] is the latest complete journal file.
      List<UfsJournalFile> journalFiles = snapshot.getLogs();
      if (!journalFiles.isEmpty()) {
        for (int i = journalFiles.size() - 1; i >= 0; i--) {
          UfsJournalFile journal = journalFiles.get(i);
          if (!journal.isIncompleteLog()) { // Do not consider incomplete logs (handled above)
            lastPersistSeq = journal.getEnd() - 1;
            LOG.info("Found last persisted journal entry with seq {} in {}.",
                lastPersistSeq, journal.getLocation().toString());
            break;
          }
        }
      }
    }
    return lastPersistSeq;
  }

  /**
   * Closes the current journal output stream and creates a new one.
   * The implementation must be idempotent so that it can work when retrying during failures.
   */
  private void maybeRotateLog() throws IOException, JournalClosedException {
    checkIsWritable();
    if (!mRotateLogForNextWrite) {
      return;
    }
    if (mJournalOutputStream != null) {
      mJournalOutputStream.close();
      mJournalOutputStream = null;
    }

    createNewLogFile(mNextSequenceNumber);
    mRotateLogForNextWrite = false;
  }

  private void createNewLogFile(long startSequenceNumber)
      throws IOException, JournalClosedException {
    checkIsWritable();
    URI newLog = UfsJournalFile
        .encodeLogFileLocation(mJournal, startSequenceNumber, UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    UfsJournalFile currentLog = UfsJournalFile.createLogFile(newLog, startSequenceNumber,
        UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    OutputStream outputStream = mUfs.create(currentLog.getLocation().toString(),
        CreateOptions.defaults(ServerConfiguration.global()).setEnsureAtomic(false)
            .setCreateParent(true));
    mJournalOutputStream = new JournalOutputStream(currentLog, outputStream);
    LOG.info("Created current log file: {}", currentLog);
  }

  /**
   * Completes the given log.
   *
   * If the log is empty, it will be deleted.
   *
   * This method must be safe to run by multiple masters at the same time. This could happen if a
   * primary master loses leadership and takes a while to close its journal. By the time it
   * completes the current log, the new primary might be trying to close it as well.
   *
   * @param currentLog the log to complete
   * @param nextSequenceNumber the next sequence number for the log to complete
   */
  private void completeLog(UfsJournalFile currentLog, long nextSequenceNumber) throws IOException {
    try {
      checkIsWritable();
    } catch (JournalClosedException e) {
      // Do not throw error, just ignore if the journal is not writable
      LOG.warn("Skipping completeLog() since journal is not writable. error: {}", e.toString());
      return;
    }
    String current = currentLog.getLocation().toString();
    if (nextSequenceNumber <= currentLog.getStart()) {
      LOG.info("No journal entry found in current journal file {}. Deleting it", current);
      if (!mUfs.deleteFile(current)) {
        LOG.warn("Failed to delete empty journal file {}", current);
      }
      return;
    }
    String completed = UfsJournalFile
        .encodeLogFileLocation(mJournal, currentLog.getStart(), nextSequenceNumber).toString();

    try {
      // Check again before the rename
      checkIsWritable();
    } catch (JournalClosedException e) {
      // Do not throw error, just ignore if the journal is not writable
      LOG.warn("Skipping completeLog() since journal is not writable. error: {}", e.toString());
      return;
    }
    LOG.info(String
        .format("Completing log %s with next sequence number %d", current, nextSequenceNumber));
    if (!mUfs.renameFile(current, completed)) {
      // Completes could happen concurrently, check whether another master already did the rename.
      if (!mUfs.exists(completed)) {
        throw new IOException(
            String.format("Failed to rename journal log from %s to %s", current, completed));
      }
      if (mUfs.exists(current)) {
        // Rename is not atomic, so this could happen if we failed partway through a rename.
        LOG.info("Deleting current log {}", current);
        if (!mUfs.deleteFile(current)) {
          LOG.warn("Failed to delete current log file {}", current);
        }
      }
    }
  }

  public synchronized void flush() throws IOException, JournalClosedException {
    checkIsWritable();
    maybeRecoverFromUfsFailures();

    if (mJournalOutputStream == null || mJournalOutputStream.bytesWritten() == 0) {
      // There is nothing to flush.
      return;
    }
    try {
      mJournalOutputStream.flush();
      // Since flush has succeeded, it's safe to clear the mEntriesToFlush queue
      // because they are considered "persisted" in UFS.
      mEntriesToFlush.clear();
    } catch (IOJournalClosedException e) {
      throw e.toJournalClosedException();
    } catch (IOException e) { // On next operation, attempt to recover from a UFS failure
      mNeedsRecovery = true;
      UfsJournalFile currentLog = mJournalOutputStream.currentLog();
      mJournalOutputStream = null;
      throw new IOException(ExceptionMessage.JOURNAL_FLUSH_FAILURE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
              currentLog, e.getMessage()), e);
    }
    boolean overSize = mJournalOutputStream.bytesWritten() >= mMaxLogSize;
    if (overSize || !mUfs.supportsFlush()) {
      // (1) The log file is oversize, needs to be rotated. Or
      // (2) Underfs is S3 or OSS, flush on S3OutputStream/OSSOutputStream will only flush to
      // local temporary file, call close and complete the log to sync the journal entry to S3/OSS.
      if (overSize) {
        LOG.info("Rotating log file {}. size: {} maxSize: {}", currentLogName(),
            mJournalOutputStream.bytesWritten(), mMaxLogSize);
      }
      mRotateLogForNextWrite = true;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    Closer closer = Closer.create();
    if (mJournalOutputStream != null) {
      closer.register(mJournalOutputStream);
    }
    closer.register(mGarbageCollector);
    closer.close();
    mClosed = true;
  }

  /**
   * A simple wrapper that wraps a output stream to the current log file. When this stream is
   * closed, the log file will be completed.
   *
   * Many of the methods in this class might throw {@link IOJournalClosedException} if the journal
   * writer is closed when they are called. The exception needs to extend IOException because the
   * OutputStream API only throws IOException. Callers of these methods should re-throw the
   * {@link IOJournalClosedException} as a regular {@link JournalClosedException} so that it will be
   * properly handled by callers.
   */
  private class JournalOutputStream extends OutputStream {
    // Not intended for use outside this inner class.
    private final DataOutputStream mOutputStream;
    private final UfsJournalFile mCurrentLog;

    JournalOutputStream(UfsJournalFile currentLog, OutputStream stream) throws IOException {
      mOutputStream = wrapDataOutputStream(stream);
      mCurrentLog = currentLog;
    }

    /**
     * @return the number of bytes written to this stream
     */
    long bytesWritten() {
      if (mOutputStream == null) {
        return 0;
      }
      return mOutputStream.size();
    }

    /**
     * @return the log file being written to by this stream
     */
    UfsJournalFile currentLog() {
      return mCurrentLog;
    }

    @Override
    public void write(int b) throws IOException {
      checkJournalWriterOpen();
      mOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      checkJournalWriterOpen();
      mOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      checkJournalWriterOpen();
      mOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      checkJournalWriterOpen();
      mOutputStream.flush();
    }

    /**
     * Closes the stream by committing the log. The implementation must be idempotent as this
     * close can fail and be retried.
     */
    @Override
    public void close() throws IOException {
      checkJournalWriterOpen();
      mOutputStream.close();
      LOG.info("Marking {} as complete with log entries within [{}, {}).",
          mCurrentLog.getLocation(), mCurrentLog.getStart(), mNextSequenceNumber);
      completeLog(mCurrentLog, mNextSequenceNumber);
    }

    private void checkJournalWriterOpen() throws IOJournalClosedException {
      if (mClosed) {
        throw new JournalClosedException("Journal writer is closed. currentLog: " + mCurrentLog)
            .toIOException();
      }
    }
  }

  private static DataOutputStream wrapDataOutputStream(OutputStream stream) {
    if (stream instanceof DataOutputStream) {
      return (DataOutputStream) stream;
    } else {
      return new DataOutputStream(stream);
    }
  }

  /**
   * @return the next sequence number to write
   */
  public synchronized long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  @VisibleForTesting
  synchronized JournalOutputStream getJournalOutputStream() {
    return mJournalOutputStream;
  }

  /**
   * @throws JournalClosedException if the journal is no longer writable
   */
  private void checkIsWritable() throws JournalClosedException {
    if (!mJournal.isWritable()) {
      throw new JournalClosedException(String
          .format("writer not allowed to write (no longer primary). location: %s currentLog: %s",
              mJournal.getLocation(), currentLogName()));
    }
  }

  private String currentLogName() {
    if (mJournalOutputStream != null) {
      return mJournalOutputStream.currentLog().toString();
    }
    return "(null output stream)";
  }
}

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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayDeque;
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
  /** The output stream to write the journal log entries. */
  private JournalOutputStream mJournalOutputStream;
  /** The garbage collector. */
  private UfsJournalGarbageCollector mGarbageCollector;
  /** Whether the journal log writer is closed. */
  private boolean mClosed;
  private Queue<JournalEntry> mEntriesToFlush;

  /**
   * A simple wrapper that wraps a output stream to the current log file.
   */
  private class JournalOutputStream implements Closeable {
    final DataOutputStream mOutputStream;
    final UfsJournalFile mCurrentLog;

    JournalOutputStream(UfsJournalFile currentLog, OutputStream stream) {
      if (stream != null) {
        if (stream instanceof DataOutputStream) {
          mOutputStream = (DataOutputStream) stream;
        } else {
          mOutputStream = new DataOutputStream(stream);
        }
      } else {
        mOutputStream = null;
      }
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
     * Closes the stream by committing the log. The implementation must be idempotent as this
     * close can fail and be retried.
     */
    @Override
    public void close() throws IOException {
      if (mOutputStream != null) {
        mOutputStream.close();
      }
      LOG.info("Marking {} as complete with log entries within [{}, {}).",
          mCurrentLog.getLocation(), mCurrentLog.getStart(), mNextSequenceNumber);

      String src = mCurrentLog.getLocation().toString();
      if (!mUfs.exists(src) && mNextSequenceNumber == mCurrentLog.getStart()) {
        // This can happen when there is any failures before creating a new log file after
        // committing last log file.
        return;
      }

      // Delete the current log if it contains nothing.
      if (mNextSequenceNumber == mCurrentLog.getStart()) {
        mUfs.deleteFile(src);
        return;
      }

      String dst = UfsJournalFile
          .encodeLogFileLocation(mJournal, mCurrentLog.getStart(), mNextSequenceNumber).toString();
      if (mUfs.exists(dst)) {
        LOG.warn("Deleting duplicate completed log {}.", dst);
        // The dst can exist because of a master failure during commit. This can only happen
        // when the primary master starts. We can delete either the src or dst. We delete dst and
        // do rename again.
        mUfs.deleteFile(dst);
      }
      mUfs.renameFile(src, dst);
    }
  }

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
    mMaxLogSize = Configuration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);

    mRotateLogForNextWrite = true;
    UfsJournalFile currentLog = UfsJournalSnapshot.getCurrentLog(mJournal);
    if (currentLog != null) {
      mJournalOutputStream = new JournalOutputStream(currentLog, null);
    }
    mGarbageCollector = new UfsJournalGarbageCollector(mJournal);
    mEntriesToFlush = new ArrayDeque<>();
  }

  public synchronized void write(JournalEntry entry) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
    }
    maybeRecoverFromUfsFailures();
    maybeRotateLog();

    try {
      entry.toBuilder().setSequenceNumber(mNextSequenceNumber).build()
          .writeDelimitedTo(mJournalOutputStream.mOutputStream);
      LOG.debug("Add this journal entry to retryList with {} entries.", mEntriesToFlush.size());
      mEntriesToFlush.add(entry);
      mNextSequenceNumber++;
    } catch (IOException e) {
      mRotateLogForNextWrite = true;
      UfsJournalFile currentLog = mJournalOutputStream.mCurrentLog;
      mJournalOutputStream = null;
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_FAILURE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
              currentLog, e.getMessage()), e);
    }
  }

  private void maybeRecoverFromUfsFailures() throws IOException {
    if (mJournalOutputStream != null) {
      return;
    }
    UfsJournalFile currentLog = UfsJournalSnapshot.getCurrentLog(mJournal);
    if (currentLog == null) {
      return;
    }
    long startSeq = currentLog.getStart();
    long lastPersistSeq = -1;
    LOG.info("Trying to recover from previous UFS journal failure."
        + " Scanning for the first corrupted journal entry.");
    try (JournalReader reader = new UfsJournalReader(mJournal, startSeq, true)) {
      JournalEntry entry;
      while ((entry = reader.read()) != null) {
        if (entry.getSequenceNumber() > lastPersistSeq) {
          lastPersistSeq = entry.getSequenceNumber();
        }
      }
    } catch (InvalidJournalEntryException e) {
      LOG.info("Found first corrupted journal entry.");
    } catch (IOException e) {
      LOG.info("I/O error encountered while trying to read journal.");
      throw e;
    }
    String src = currentLog.getLocation().toString();
    LOG.info("Last valid (persisted) journal entry sequence number in {} is {}",
        src, lastPersistSeq);
    if (lastPersistSeq < 0) {
      mUfs.deleteFile(src);
      return;
    }

    String dst = UfsJournalFile
        .encodeLogFileLocation(mJournal, currentLog.getStart(), lastPersistSeq + 1).toString();
    if (mUfs.exists(dst)) {
      LOG.warn("Deleting duplicate completed log {}.", dst);
      // The dst can exist because of a master failure during commit. This can only happen
      // when the primary master starts. We can delete either the src or dst. We delete dst and
      // do rename again.
      mUfs.deleteFile(dst);
    }
    LOG.info("Renaming the previous incomplete journal file.");
    mUfs.renameFile(src, dst);

    maybeRotateLog();
    if (!mEntriesToFlush.isEmpty()) {
      JournalEntry entry = mEntriesToFlush.peek();
      if (entry.getSequenceNumber() > lastPersistSeq + 1) {
        LOG.error("Journal entries between [{}, {}) are missing. Exiting.",
            lastPersistSeq + 1, entry.getSequenceNumber());
        throw new IOException(ExceptionMessage.JOURNAL_ENTRY_MISSING
            .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
                currentLog));
      }
      LOG.info("Retry writing unwritten journal entries");
    }
    for (JournalEntry entry : mEntriesToFlush) {
      if (entry.getSequenceNumber() > lastPersistSeq) {
        try {
          entry.toBuilder().build()
              .writeDelimitedTo(mJournalOutputStream.mOutputStream);
        } catch (IOException e) {
          mRotateLogForNextWrite = true;
          UfsJournalFile tempLog = mJournalOutputStream.mCurrentLog;
          mJournalOutputStream = null;
          throw new IOException(ExceptionMessage.JOURNAL_WRITE_FAILURE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
              tempLog, e.getMessage()), e);
        }
      }
    }
    if (!mEntriesToFlush.isEmpty()) {
      LOG.info("Finished writing unwritten journal entries.");
    }
  }

  /**
   * Closes the current journal output stream and creates a new one.
   * The implementation must be idempotent so that it can work when retrying during failures.
   */
  private void maybeRotateLog() throws IOException {
    if (!mRotateLogForNextWrite) {
      return;
    }
    if (mJournalOutputStream != null) {
      mJournalOutputStream.close();
      mJournalOutputStream = null;
    }

    URI newLog = UfsJournalFile
        .encodeLogFileLocation(mJournal, mNextSequenceNumber, UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    UfsJournalFile currentLog = UfsJournalFile.createLogFile(newLog, mNextSequenceNumber,
        UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    OutputStream outputStream = mUfs.create(currentLog.getLocation().toString(),
        CreateOptions.defaults().setEnsureAtomic(false).setCreateParent(true));
    mJournalOutputStream = new JournalOutputStream(currentLog, outputStream);
    LOG.info("Created current log file: {}", currentLog);
    mRotateLogForNextWrite = false;
  }

  public synchronized void flush() throws IOException {
    if (mClosed || mJournalOutputStream == null || mJournalOutputStream.bytesWritten() == 0) {
      // There is nothing to flush.
      return;
    }
    DataOutputStream outputStream = mJournalOutputStream.mOutputStream;
    try {
      outputStream.flush();
      mEntriesToFlush.clear();
    } catch (IOException e) {
      mRotateLogForNextWrite = true;
      UfsJournalFile currentLog = mJournalOutputStream.mCurrentLog;
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
        LOG.info("Rotating log file. size: {} maxSize: {}", mJournalOutputStream.bytesWritten(),
            mMaxLogSize);
      }
      mRotateLogForNextWrite = true;
    }
  }

  public synchronized void close() throws IOException {
    Closer closer = Closer.create();
    if (mJournalOutputStream != null) {
      closer.register(mJournalOutputStream);
    }
    closer.register(mGarbageCollector);
    closer.close();
    mClosed = true;
  }
}

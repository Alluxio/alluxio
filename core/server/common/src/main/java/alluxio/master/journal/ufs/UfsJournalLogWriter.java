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
import alluxio.exception.ExceptionMessage;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalWriterCreateOptions;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.options.CreateOptions;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link JournalWriter} based on UFS.
 */
@ThreadSafe
public final class UfsJournalLogWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalLogWriter.class);

  private final UfsJournal mJournal;
  private final long mMaxLogSize;

  private long mNextSequenceNumber;
  /**
   * When mRotateForNextWrite is set to true, mJournalOutputStream must be closed
   * cleared before the next write.
   */
  private boolean mRotateLogForNextWrite;
  private JournalOutputStream mJournalOutputStream;
  private UfsJournalGarbageCollector mGarbageCollector;

  private class JournalOutputStream implements Closeable {
    final DataOutputStream mOutputStream;
    final UfsJournalFile mCurrentLog;

    JournalOutputStream(UfsJournalFile currentLog, OutputStream stream) {
      if (stream instanceof DataOutputStream) {
        mOutputStream = (DataOutputStream) stream;
      } else {
        mOutputStream = new DataOutputStream(stream);
      }
      mCurrentLog = currentLog;
    }

    long bytesWritten() {
      return mOutputStream.size();
    }

    /**
     * Closes the stream by committing the log. The implementation must be idempotent as this
     * close can fail and be retried.
     *
     * @throws IOException if it fails to close
     */
    @Override
    public void close() throws IOException {
      if (mOutputStream != null) {
        mOutputStream.close();
      }
      LOG.info("Marking {} as complete with log entries within [{}, {}).", mCurrentLog.getLocation(),
          mCurrentLog.getStart(), mNextSequenceNumber);


      String src = mCurrentLog.getLocation().toString();
      if (!mJournal.getUfs().exists(src) && mNextSequenceNumber == mCurrentLog.getStart()) {
        // This can happen when there is any failures before creating a new log file after
        // committing last log file.
        return;
      }

      // Delete the current log if it contains nothing.
      if (mNextSequenceNumber == mCurrentLog.getStart()) {
        mJournal.getUfs().deleteFile(src);
        return;
      }

      String dst =
          mJournal.getCheckpointOrLogFileLocation(mCurrentLog.getStart(), mNextSequenceNumber, false)
              .toString();
      if (mJournal.getUfs().exists(dst)) {
        LOG.warn("Deleting duplicate completed log {}.", dst);
        // The dst can exist because of a master failure during commit. This can only happen
        // when the primary master starts. We can delete either the src or dst. We delete dst and
        // do rename again.
        mJournal.getUfs().deleteFile(dst);
      }
      mJournal.getUfs().renameFile(src, dst);
    }
  }

  private boolean mClosed;

  /**
   * Creates a new instance of {@link UfsJournalLogWriter}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalLogWriter(UfsJournal journal, JournalWriterCreateOptions options) throws IOException {
    mJournal = Preconditions.checkNotNull(journal);
    mNextSequenceNumber = options.getNextSequenceNumber();
    mMaxLogSize = Configuration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);

    UfsJournalFile currentLog = mJournal.getCurrentLog();
    if (currentLog != null) {
      mRotateLogForNextWrite = true;
      mJournalOutputStream = new JournalOutputStream(currentLog, null);
    }
    mGarbageCollector = new UfsJournalGarbageCollector(mJournal);
  }

  @Override
  public synchronized void write(JournalEntry entry) throws IOException {
    if (mClosed) {
      throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
    }
    maybeRotateLog();

    try {
      entry.toBuilder().setSequenceNumber(mNextSequenceNumber).build()
          .writeDelimitedTo(mJournalOutputStream.mOutputStream);
    } catch (IOException e) {
      mRotateLogForNextWrite = true;
      throw e;
    }
    mNextSequenceNumber++;
  }

  /**
   * Closes the current journal output stream and creates a new one.
   *
   * The implementation must be idempotent so that it can work when retrying during failures.
   *
   * @throws IOException if an IO exception occurs during the log rotation
   */
  private void maybeRotateLog() throws IOException {
    if (!mRotateLogForNextWrite) return;

    mJournalOutputStream.close();
    mJournalOutputStream = null;

    URI new_log = mJournal
        .getCheckpointOrLogFileLocation(mNextSequenceNumber, UfsJournal.UNKNOWN_SEQUENCE_NUMBER,
            false  /* is_checkpoint */);
    UfsJournalFile currentLog =
        UfsJournalFile.createLog(new_log, mNextSequenceNumber, UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    OutputStream outputStream = mJournal.getUfs().create(currentLog.getLocation().toString(),
        CreateOptions.defaults().setEnsureAtomic(false).setCreateParent(true));
    mJournalOutputStream = new JournalOutputStream(currentLog, outputStream);
    LOG.info("Opened current log file: {}", currentLog);
    mRotateLogForNextWrite = false;
  }

  @Override
  public synchronized void flush() throws IOException {
    if (mClosed || mJournalOutputStream == null || mJournalOutputStream.bytesWritten() == 0) {
      // There is nothing to flush.
      return;
    }
    DataOutputStream outputStream = mJournalOutputStream.mOutputStream;
    try {
      outputStream.flush();
      if (outputStream instanceof FSDataOutputStream) {
        // The output stream directly created by {@link UnderFileSystem} may be
        // {@link FSDataOutputStream}, which means the under filesystem is HDFS, but
        // {@link DataOutputStream#flush} won't flush the data to HDFS, so we need to call
        // {@link FSDataOutputStream#sync} to actually flush data to HDFS.
        ((FSDataOutputStream) outputStream).sync();
      }
    } catch (IOException e) {
      mRotateLogForNextWrite = true;
      throw e;
    }
    boolean overSize = mJournalOutputStream.bytesWritten() >= mMaxLogSize;
    if (overSize || !mJournal.getUfs().supportsFlush()) {
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

  @Override
  public synchronized void cancel() throws IOException {
    throw new UnsupportedOperationException("UfsJournalLogWriter#cancel is not supported.");
  }
}

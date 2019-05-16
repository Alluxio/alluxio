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

package alluxio.master.journalv0.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.ExceptionMessage;
import alluxio.master.journalv0.JournalFormatter;
import alluxio.master.journalv0.JournalOutputStream;
import alluxio.master.journalv0.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link JournalWriter} based on UFS.
 */
@ThreadSafe
public final class UfsJournalWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalWriter.class);

  private final UfsJournal mJournal;
  /** Location storing all completed logs. */
  private final URI mCompletedLocation;
  /**
   * Location to the temporary checkpoint. This is where a new checkpoint file is fully
   * written before being moved to the actual checkpoint location.
   */
  private final URI mTempCheckpoint;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  /** The log number to assign to the next complete log. */
  private long mNextCompleteLogNumber = UfsJournal.FIRST_COMPLETED_LOG_NUMBER;

  /** The output stream singleton for the checkpoint file. */
  private CheckpointOutputStream mCheckpointOutputStream = null;
  /** The output stream singleton for the entry log files. */
  private EntryOutputStream mEntryOutputStream = null;

  /** The sequence number for the next entry in the log. */
  private long mNextEntrySequenceNumber = 1;

  /** Checkpoint manager for updating and recovering the checkpoint file. */
  private UfsCheckpointManager mCheckpointManager;

  /**
   * Creates a new instance of {@link UfsJournalWriter}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalWriter(UfsJournal journal) {
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mCompletedLocation = mJournal.getCompletedLocation();
    try {
      mTempCheckpoint = new URI(mJournal.getCheckpoint() + ".tmp");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    mUfs = UnderFileSystem.Factory.create(mJournal.getLocation().toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    mCheckpointManager = new UfsCheckpointManager(mUfs, mJournal.getCheckpoint(), this);
  }

  @Override
  public synchronized void completeLogs() throws IOException {
    LOG.info("Marking all logs as complete.");
    // Loop over all complete logs starting from the beginning, to determine the next log number.
    mNextCompleteLogNumber = UfsJournal.FIRST_COMPLETED_LOG_NUMBER;
    URI log = mJournal.getCompletedLog(mNextCompleteLogNumber);
    while (mUfs.isFile(log.toString())) {
      mNextCompleteLogNumber++;
      // generate the next completed log filename in the sequence.
      log = mJournal.getCompletedLog(mNextCompleteLogNumber);
    }
    completeCurrentLog();
  }

  @Override
  public synchronized JournalOutputStream getCheckpointOutputStream(long latestSequenceNumber)
      throws IOException {
    if (mCheckpointOutputStream == null) {
      mCheckpointManager.recover();
      LOG.info("Creating tmp checkpoint file: {}", mTempCheckpoint);
      if (!mUfs.isDirectory(mJournal.getLocation().toString())) {
        LOG.info("Creating journal folder: {}", mJournal.getLocation());
        mUfs.mkdirs(mJournal.getLocation().toString());
      }
      mNextEntrySequenceNumber = latestSequenceNumber + 1;
      LOG.info("Latest journal sequence number: {} Next journal sequence number: {}",
          latestSequenceNumber, mNextEntrySequenceNumber);
      UnderFileSystemUtils.deleteFileIfExists(mUfs, mTempCheckpoint.toString());
      mCheckpointOutputStream = new CheckpointOutputStream(
          new DataOutputStream(mUfs.create(mTempCheckpoint.toString())));
    }
    return mCheckpointOutputStream;
  }

  @Override
  public synchronized void write(JournalEntry entry) throws IOException {
    if (mCheckpointOutputStream == null || !mCheckpointOutputStream.isClosed()) {
      throw new IOException("The checkpoint must be written and closed before writing entries.");
    }
    if (mEntryOutputStream == null) {
      mEntryOutputStream = new EntryOutputStream(mUfs, mJournal.getCurrentLog(),
          mJournal.getJournalFormatter(), this);
    }
    mEntryOutputStream.write(entry);
  }

  @Override
  public synchronized void flush() throws IOException {
    if (mCheckpointOutputStream == null || !mCheckpointOutputStream.isClosed()) {
      throw new IOException("The checkpoint must be written and closed before writing entries.");
    }
    if (mEntryOutputStream == null) { // no entries to flush
      return;
    }
    mEntryOutputStream.flush();
  }

  @Override
  public synchronized long getNextSequenceNumber() {
    return mNextEntrySequenceNumber++;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mCheckpointOutputStream != null) {
      mCheckpointOutputStream.close();
    }
    if (mEntryOutputStream != null) {
      mEntryOutputStream.close();
    }
    // Close the ufs.
    mUfs.close();
  }

  @Override
  public void recover() {
    mCheckpointManager.recover();
  }

  @Override
  public synchronized void deleteCompletedLogs() throws IOException {
    LOG.info("Deleting all completed log files...");
    // Loop over all complete logs starting from the end.
    long logNumber = UfsJournal.FIRST_COMPLETED_LOG_NUMBER;
    while (mUfs.isFile(mJournal.getCompletedLog(logNumber).toString())) {
      logNumber++;
    }
    for (long i = logNumber - 1; i >= 0; i--) {
      URI log = mJournal.getCompletedLog(i);
      LOG.info("Deleting completed log: {}", log);
      mUfs.deleteFile(log.toString());
    }
    LOG.info("Finished deleting all completed log files.");

    // All complete logs are deleted. Reset the log number counter.
    mNextCompleteLogNumber = UfsJournal.FIRST_COMPLETED_LOG_NUMBER;
  }

  @Override
  public synchronized void completeCurrentLog() throws IOException {
    URI currentLog = mJournal.getCurrentLog();
    if (!mUfs.isFile(currentLog.toString())) {
      // All logs are already complete, so nothing to do.
      return;
    }

    if (!mUfs.isDirectory(mCompletedLocation.toString())) {
      mUfs.mkdirs(mCompletedLocation.toString());
    }

    URI completedLog = mJournal.getCompletedLog(mNextCompleteLogNumber);
    mUfs.renameFile(currentLog.toString(), completedLog.toString());
    LOG.info("Completed current log: {} to completed log: {}", currentLog, completedLog);

    mNextCompleteLogNumber++;
  }

  /**
   * This is the output stream for the journal checkpoint file. When this stream is closed, it will
   * delete the completed logs, and then mark the current log as complete.
   */
  private class CheckpointOutputStream implements JournalOutputStream {
    private final DataOutputStream mOutputStream;
    private boolean mIsClosed = false;

    CheckpointOutputStream(DataOutputStream outputStream) {
      mOutputStream = outputStream;
    }

    boolean isClosed() {
      return mIsClosed;
    }

    /**
     * Writes an entry to the checkpoint file.
     *
     * The entry should not have its sequence number set. This method will add the proper sequence
     * number to the passed in entry.
     *
     * @param entry an entry to write to the journal checkpoint file
     */
    @Override
    public synchronized void write(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
      }
      mJournal.getJournalFormatter().serialize(
          entry.toBuilder().setSequenceNumber(getNextSequenceNumber()).build(),
          mOutputStream);
    }

    /**
     * Closes the checkpoint file. The entries in the checkpoint should already reflect all the
     * state of the complete log files (but not the last, current log file).
     *
     * Closing the checkpoint file will delete all the completed log files, since the checkpoint
     * already reflects that state.
     *
     * The current log file (if it exists) will be closed and marked as complete.
     */
    @Override
    public synchronized void close() throws IOException {
      if (mIsClosed) {
        return;
      }
      mOutputStream.flush();
      mOutputStream.close();

      LOG.info("Successfully created tmp checkpoint file: {}", mTempCheckpoint);

      mCheckpointManager.update(mTempCheckpoint);

      // Consider the current log to be complete.
      completeCurrentLog();

      mIsClosed = true;
    }

    @Override
    public synchronized void flush() throws IOException {
      if (mIsClosed) {
        return;
      }
      mOutputStream.flush();
    }
  }

  /**
   * This is the output stream for the journal entries after the checkpoint. This output stream
   * handles rotating log files, and creating the next log file.
   *
   * Log files are rotated in the following scenarios:
   *
   * <pre>
   * 1. The log size reaches {@link #mMaxLogSize}
   * 2. {@link #flush()} is called but the journal is in a UFS which doesn't support flush.
   *    Closing the file is the only way to simulate a flush.
   * 3. An IO exception occurs while writing to or flushing an entry. We must rotate here in case
   *    corrupted data was written. When reading the log we will detect corruption and assume that
   *    it's at the end of the log.
   * </pre>
   */
  @ThreadSafe
  protected static class EntryOutputStream implements JournalOutputStream {
    private final UnderFileSystem mUfs;
    private final URI mCurrentLog;
    private final JournalFormatter mJournalFormatter;
    private final UfsJournalWriter mJournalWriter;
    private final long mMaxLogSize;

    /** The direct output stream created by {@link UnderFileSystem}. */
    private OutputStream mRawOutputStream;
    /** The output stream that wraps around {@link #mRawOutputStream}. */
    private DataOutputStream mDataOutputStream;
    private boolean mIsClosed = false;
    /**
     * When true, the current log must be rotated before writing the next entry. This is set when
     * the previous write failed and may have left a corrupted entry at the end of the current log.
     */
    private boolean mRotateLogForNextWrite = false;

    /**
     * @param ufs the under storage holding the journal
     * @param log the location to write the log to
     * @param journalFormatter the journal formatter to use when writing journal entries
     * @param journalWriter the journal writer to use to get journal entry sequence numbers and
     *        complete the log when it needs to be rotated
     */
    public EntryOutputStream(UnderFileSystem ufs, URI log, JournalFormatter journalFormatter,
        UfsJournalWriter journalWriter) throws IOException {
      mUfs = ufs;
      mCurrentLog = log;
      mJournalFormatter = journalFormatter;
      mJournalWriter = journalWriter;
      mMaxLogSize = ServerConfiguration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);
      mRawOutputStream = mUfs.create(mCurrentLog.toString(),
          CreateOptions.defaults(ServerConfiguration.global())
              .setEnsureAtomic(false).setCreateParent(true));
      LOG.info("Opened current log file: {}", mCurrentLog);
      mDataOutputStream = new DataOutputStream(mRawOutputStream);
    }

    /**
     * The given entry should not have its sequence number set. This method will add the proper
     * sequence number to the passed in entry.
     *
     * @param entry an entry to write to the journal checkpoint file
     */
    @Override
    public synchronized void write(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
      }
      if (mRotateLogForNextWrite) {
        rotateLog();
        mRotateLogForNextWrite = false;
      }
      try {
        mJournalFormatter.serialize(
            entry.toBuilder().setSequenceNumber(mJournalWriter.getNextSequenceNumber()).build(),
            mDataOutputStream);
      } catch (IOException e) {
        mRotateLogForNextWrite = true;
        throw new IOException(ExceptionMessage.JOURNAL_WRITE_FAILURE.getMessageWithUrl(
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mCurrentLog, e.getMessage()), e);
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (mIsClosed) {
        return;
      }
      if (mDataOutputStream != null) {
        // Close the current log file.
        mDataOutputStream.close();
      }

      mIsClosed = true;
    }

    @Override
    public synchronized void flush() throws IOException {
      if (mIsClosed || mDataOutputStream.size() == 0) {
        // There is nothing to flush.
        return;
      }
      try {
        mDataOutputStream.flush();
      } catch (IOException e) {
        mRotateLogForNextWrite = true;
        throw new IOException(ExceptionMessage.JOURNAL_FLUSH_FAILURE.getMessageWithUrl(
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mCurrentLog, e.getMessage()), e);
      }
      boolean overSize = mDataOutputStream.size() >= mMaxLogSize;
      if (overSize || !mUfs.supportsFlush()) {
        // (1) The log file is oversize, needs to be rotated. Or
        // (2) Underfs is S3 or OSS, flush on S3OutputStream/OSSOutputStream will only flush to
        // local temporary file,
        // call close and complete the log to sync the journal entry to S3/OSS.
        if (overSize) {
          LOG.info("Rotating log file. size: {} maxSize: {}", mDataOutputStream.size(),
              mMaxLogSize);
        }
        mRotateLogForNextWrite = true;
      }
    }

    /**
     * Completes the current log and rotates in a new log.
     */
    private void rotateLog() throws IOException {
      mDataOutputStream.close();
      mJournalWriter.completeCurrentLog();
      mRawOutputStream = mUfs.create(mCurrentLog.toString(),
          CreateOptions.defaults(ServerConfiguration.global()).setEnsureAtomic(false)
              .setCreateParent(true));
      LOG.info("Opened current log file: {}", mCurrentLog);
      mDataOutputStream = new DataOutputStream(mRawOutputStream);
    }
  }
}

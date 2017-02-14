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

package alluxio.master.journal;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class manages all the writes to the journal. Journal writes happen in two phases:
 *
 * 1. First the checkpoint file is written. The checkpoint file contains entries reflecting the
 * state of the master with all of the completed logs applied.
 *
 * 2. Afterwards, entries are appended to log files. The checkpoint file must be written before the
 * log files.
 *
 * The latest state can be reconstructed by reading the checkpoint file, and applying all the
 * completed logs and then the remaining log in progress.
 */
@ThreadSafe
public final class JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  /** Absolute path to the directory storing all of the journal data. */
  private final String mJournalDirectory;
  /** Absolute path to the directory storing all completed logs. */
  private final String mCompletedDirectory;
  /**
   * Absolute path to the temporary checkpoint file. This is where a new checkpoint file is fully
   * written before being renamed to {@link #mCheckpointPath}
   */
  private final String mTempCheckpointPath;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  /** The log number to assign to the next complete log. */
  private long mNextCompleteLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;

  /** The output stream singleton for the checkpoint file. */
  private CheckpointOutputStream mCheckpointOutputStream = null;
  /** The output stream singleton for the entry log files. */
  private EntryOutputStream mEntryOutputStream = null;

  /** The sequence number for the next entry in the log. */
  private long mNextEntrySequenceNumber = 1;

  /** Checkpoint manager for updating and recovering the checkpoint file. */
  private CheckpointManager mCheckpointManager;

  /**
   * Creates a new instance of {@link JournalWriter}.
   *
   * @param journal the handle to the journal
   */
  JournalWriter(Journal journal) {
    mJournal = Preconditions.checkNotNull(journal);
    mJournalDirectory = mJournal.getDirectory();
    mCompletedDirectory = mJournal.getCompletedDirectory();
    mTempCheckpointPath = mJournal.getCheckpointFilePath() + ".tmp";
    mUfs = UnderFileSystem.Factory.get(mJournalDirectory);
    mCheckpointManager = new CheckpointManager(mUfs, mJournal.getCheckpointFilePath(), this);
  }

  /**
   * Marks all logs as completed.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completeAllLogs() throws IOException {
    LOG.info("Marking all logs as complete.");
    // Loop over all complete logs starting from the beginning, to determine the next log number.
    mNextCompleteLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;
    String logFilename = mJournal.getCompletedLogFilePath(mNextCompleteLogNumber);
    while (mUfs.isFile(logFilename)) {
      mNextCompleteLogNumber++;
      // generate the next completed log filename in the sequence.
      logFilename = mJournal.getCompletedLogFilePath(mNextCompleteLogNumber);
    }
    completeCurrentLog();
  }

  /**
   * Returns an output stream for the journal checkpoint. The returned output stream is a singleton
   * for this writer.
   *
   * @param latestSequenceNumber the sequence number of the latest journal entry. This sequence
   *        number will be used to determine the next sequence numbers for the subsequent journal
   *        entries.
   * @return the output stream for the journal checkpoint
   * @throws IOException if an I/O error occurs
   */
  public synchronized JournalOutputStream getCheckpointOutputStream(long latestSequenceNumber)
      throws IOException {
    if (mCheckpointOutputStream == null) {
      mCheckpointManager.recoverCheckpoint();
      LOG.info("Creating tmp checkpoint file: {}", mTempCheckpointPath);
      if (!mUfs.isDirectory(mJournalDirectory)) {
        LOG.info("Creating journal folder: {}", mJournalDirectory);
        mUfs.mkdirs(mJournalDirectory);
      }
      mNextEntrySequenceNumber = latestSequenceNumber + 1;
      LOG.info("Latest journal sequence number: {} Next journal sequence number: {}",
          latestSequenceNumber, mNextEntrySequenceNumber);
      UnderFileSystemUtils.deleteFileIfExists(mTempCheckpointPath);
      mCheckpointOutputStream =
          new CheckpointOutputStream(new DataOutputStream(mUfs.create(mTempCheckpointPath)));
    }
    return mCheckpointOutputStream;
  }

  /**
   * Writes an entry to the current {@link EntryOutputStream} if one is active. Otherwise a new
   * stream is created and the entry is written to it. {@link #flushEntryStream} should be called
   * afterward to ensure the entry is written to the underlying storage.
   *
   * @param entry the journal entry to write
   * @throws IOException if an error occurs writing the entry or if the checkpoint is not closed
   */
  public synchronized void writeEntry(JournalEntry entry) throws IOException {
    if (mCheckpointOutputStream == null || !mCheckpointOutputStream.isClosed()) {
      throw new IOException("The checkpoint must be written and closed before writing entries.");
    }
    if (mEntryOutputStream == null) {
      mEntryOutputStream = new EntryOutputStream(mUfs, mJournal.getCurrentLogFilePath(),
          mJournal.getJournalFormatter(), this);
    }
    mEntryOutputStream.writeEntry(entry);
  }

  /**
   * Flushes the current {@link EntryOutputStream} if one is active. Otherwise this operation is
   * a no-op.
   *
   * @throws IOException if an error occurs preventing the stream from being flushed
   */
  public synchronized void flushEntryStream() throws IOException {
    if (mCheckpointOutputStream == null || !mCheckpointOutputStream.isClosed()) {
      throw new IOException("The checkpoint must be written and closed before writing entries.");
    }
    if (mEntryOutputStream == null) { // no entries to flush
      return;
    }
    mEntryOutputStream.flush();
  }

  /**
   * @return the next entry sequence number
   */
  public synchronized long allocateNextEntrySequenceNumber() {
    return mNextEntrySequenceNumber++;
  }

  /**
   * Closes the journal.
   *
   * @throws IOException if an I/O error occurs
   */
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

  /**
   * Recovers the checkpoint file in case the master crashed while updating it previously.
   */
  public void recoverCheckpoint() {
    mCheckpointManager.recoverCheckpoint();
  }

  /**
   * Deletes all of the logs in the completed folder.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void deleteCompletedLogs() throws IOException {
    LOG.info("Deleting all completed log files...");
    // Loop over all complete logs starting from the end.
    long logNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;
    while (mUfs.isFile(mJournal.getCompletedLogFilePath(logNumber))) {
      logNumber++;
    }
    for (long i = logNumber - 1; i >= 0; i--) {
      String logFilename = mJournal.getCompletedLogFilePath(i);
      LOG.info("Deleting completed log: {}", logFilename);
      mUfs.deleteFile(logFilename);
    }
    LOG.info("Finished deleting all completed log files.");

    // All complete logs are deleted. Reset the log number counter.
    mNextCompleteLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;
  }

  /**
   * Moves the current log file to the completed folder, marking it as complete. If successful, the
   * current log file will no longer exist. The current log must already be closed before this call.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completeCurrentLog() throws IOException {
    String currentLog = mJournal.getCurrentLogFilePath();
    if (!mUfs.isFile(currentLog)) {
      // All logs are already complete, so nothing to do.
      return;
    }

    if (!mUfs.isDirectory(mCompletedDirectory)) {
      mUfs.mkdirs(mCompletedDirectory);
    }

    String completedLog = mJournal.getCompletedLogFilePath(mNextCompleteLogNumber);
    mUfs.renameFile(currentLog, completedLog);
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
     * @throws IOException if an I/O error occurs
     */
    @Override
    public synchronized void writeEntry(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
      }
      mJournal.getJournalFormatter().serialize(
          entry.toBuilder().setSequenceNumber(allocateNextEntrySequenceNumber()).build(),
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
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public synchronized void close() throws IOException {
      if (mIsClosed) {
        return;
      }
      mOutputStream.flush();
      mOutputStream.close();

      LOG.info("Successfully created tmp checkpoint file: {}", mTempCheckpointPath);

      mCheckpointManager.updateCheckpoint(mTempCheckpointPath);

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
   * 1. The log size reaches {@link mMaxSize}
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
    private final String mCurrentLogPath;
    private final JournalFormatter mJournalFormatter;
    private final JournalWriter mJournalWriter;
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
     * @param logPath the path to write the log to
     * @param journalFormatter the journal formatter to use when writing journal entries
     * @param journalWriter the journal writer to use to get journal entry sequence numbers and
     *        complete the log when it needs to be rotated
     * @throws IOException if the ufs can't create an outstream to logPath
     */
    public EntryOutputStream(UnderFileSystem ufs, String logPath, JournalFormatter journalFormatter,
        JournalWriter journalWriter) throws IOException {
      mUfs = ufs;
      mCurrentLogPath = logPath;
      mJournalFormatter = journalFormatter;
      mJournalWriter = journalWriter;
      mMaxLogSize = Configuration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);
      mRawOutputStream = mUfs.create(mCurrentLogPath,
          CreateOptions.defaults().setEnsureAtomic(false).setCreateParent(true));
      LOG.info("Opened current log file: {}", mCurrentLogPath);
      mDataOutputStream = new DataOutputStream(mRawOutputStream);
    }

    /**
     * The given entry should not have its sequence number set. This method will add the proper
     * sequence number to the passed in entry.
     *
     * @param entry an entry to write to the journal checkpoint file
     * @throws IOException if an I/O error occurs
     */
    @Override
    public synchronized void writeEntry(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException(ExceptionMessage.JOURNAL_WRITE_AFTER_CLOSE.getMessage());
      }
      if (mRotateLogForNextWrite) {
        rotateLog();
        mRotateLogForNextWrite = false;
      }
      try {
        mJournalFormatter
            .serialize(
                entry.toBuilder()
                    .setSequenceNumber(mJournalWriter.allocateNextEntrySequenceNumber()).build(),
                mDataOutputStream);
      } catch (IOException e) {
        mRotateLogForNextWrite = true;
        throw e;
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
        if (mRawOutputStream instanceof FSDataOutputStream) {
          // The output stream directly created by {@link UnderFileSystem} may be
          // {@link FSDataOutputStream}, which means the under filesystem is HDFS, but
          // {@link DataOutputStream#flush} won't flush the data to HDFS, so we need to call
          // {@link FSDataOutputStream#sync} to actually flush data to HDFS.
          ((FSDataOutputStream) mRawOutputStream).sync();
        }
      } catch (IOException e) {
        mRotateLogForNextWrite = true;
        throw e;
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
     *
     * @throws IOException if an IO exception occurs during the log rotation
     */
    private void rotateLog() throws IOException {
      mDataOutputStream.close();
      mJournalWriter.completeCurrentLog();
      mRawOutputStream = mUfs.create(mCurrentLogPath,
          CreateOptions.defaults().setEnsureAtomic(false).setCreateParent(true));
      LOG.info("Opened current log file: {}", mCurrentLogPath);
      mDataOutputStream = new DataOutputStream(mRawOutputStream);
    }
  }
}

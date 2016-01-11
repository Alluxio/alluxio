/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.master.MasterContext;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.underfs.UnderFileSystem;

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
public final class JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  /** Absolute path to the directory storing all of the journal data. */
  private final String mJournalDirectory;
  /** Absolute path to the directory storing all completed logs. */
  private final String mCompletedDirectory;
  /** Absolute path to the temporary checkpoint file. */
  private final String mTempCheckpointPath;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  private final long mMaxLogSize;

  /** The log number to assign to the next complete log. */
  private long mNextCompleteLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;

  /** The output stream singleton for the checkpoint file. */
  private CheckpointOutputStream mCheckpointOutputStream = null;
  /** The output stream singleton for the entry log files. */
  private EntryOutputStream mEntryOutputStream = null;

  /** The sequence number for the next entry in the log. */
  private long mNextEntrySequenceNumber = 1;

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
    TachyonConf conf = MasterContext.getConf();
    mUfs = UnderFileSystem.get(mJournalDirectory, conf);
    mMaxLogSize = conf.getBytes(Constants.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);
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
    while (mUfs.exists(logFilename)) {
      mNextCompleteLogNumber ++;
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
      LOG.info("Creating tmp checkpoint file: {}", mTempCheckpointPath);
      if (!mUfs.exists(mJournalDirectory)) {
        LOG.info("Creating journal folder: {}", mJournalDirectory);
        mUfs.mkdirs(mJournalDirectory, true);
      }
      mNextEntrySequenceNumber = latestSequenceNumber + 1;
      LOG.info("Latest journal sequence number: {} Next journal sequence number: {}",
          latestSequenceNumber, mNextEntrySequenceNumber);
      mCheckpointOutputStream =
          new CheckpointOutputStream(new DataOutputStream(mUfs.create(mTempCheckpointPath)));
    }
    return mCheckpointOutputStream;
  }

  /**
   * Returns an output stream for the journal entries. The returned output stream is a singleton for
   * this writer.
   *
   * @return the output stream for the journal entries
   * @throws IOException if an I/O error occurs
   */
  public synchronized JournalOutputStream getEntryOutputStream() throws IOException {
    if (mCheckpointOutputStream == null || !mCheckpointOutputStream.isClosed()) {
      throw new IOException("The checkpoint must be written and closed before writing entries.");
    }
    if (mEntryOutputStream == null) {
      mEntryOutputStream = new EntryOutputStream(openCurrentLog());
    }
    return mEntryOutputStream;
  }

  /**
   * Closes the journal.
   *
   * @throws IOException if an I/O error occurs
   */
  public void close() throws IOException {
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
   * Returns the current log file output stream.
   *
   * @return the output stream for the current log file
   * @throws IOException if an I/O error occurs
   */
  private OutputStream openCurrentLog() throws IOException {
    String currentLogFile = mJournal.getCurrentLogFilePath();
    OutputStream os = mUfs.create(currentLogFile);
    LOG.info("Opened current log file: {}", currentLogFile);
    return os;
  }

  /**
   * Deletes all of the logs in the completed folder.
   *
   * @throws IOException if an I/O error occurs
   */
  private void deleteCompletedLogs() throws IOException {
    LOG.info("Deleting all completed log files...");
    // Loop over all complete logs starting from the beginning.
    // TODO(gpang): should the deletes start from the end?
    long logNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;
    String logFilename = mJournal.getCompletedLogFilePath(logNumber);
    while (mUfs.exists(logFilename)) {
      LOG.info("Deleting completed log: {}", logFilename);
      mUfs.delete(logFilename, true);
      logNumber ++;
      // generate the next completed log filename in the sequence.
      logFilename = mJournal.getCompletedLogFilePath(logNumber);
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
  private void completeCurrentLog() throws IOException {
    String currentLog = mJournal.getCurrentLogFilePath();
    if (!mUfs.exists(currentLog)) {
      // All logs are already complete, so nothing to do.
      return;
    }

    if (!mUfs.exists(mCompletedDirectory)) {
      mUfs.mkdirs(mCompletedDirectory, true);
    }

    String completedLog = mJournal.getCompletedLogFilePath(mNextCompleteLogNumber);
    mUfs.rename(currentLog, completedLog);
    LOG.info("Completed current log: {} to completed log: {}", currentLog, completedLog);

    mNextCompleteLogNumber ++;
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
          entry.toBuilder().setSequenceNumber(mNextEntrySequenceNumber ++).build(), mOutputStream);
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
      mUfs.delete(mJournal.getCheckpointFilePath(), false);
      // TODO(gpang): the real checkpoint should not be overwritten here, but after all operations.
      mUfs.rename(mTempCheckpointPath, mJournal.getCheckpointFilePath());
      mUfs.delete(mTempCheckpointPath, false);
      LOG.info("Renamed checkpoint file {} to {}", mTempCheckpointPath,
          mJournal.getCheckpointFilePath());

      // The checkpoint already reflects the information in the completed logs.
      deleteCompletedLogs();

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
   * handles rotating full log files, and creating the next log file.
   */
  private class EntryOutputStream implements JournalOutputStream {
    /** The direct output stream created by {@link UnderFileSystem} */
    private OutputStream mRawOutputStream;
    /** The output stream that wraps around {@link #mRawOutputStream} */
    private DataOutputStream mDataOutputStream;
    private boolean mIsClosed = false;

    EntryOutputStream(OutputStream outputStream) {
      mRawOutputStream = outputStream;
      mDataOutputStream = new DataOutputStream(outputStream);
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
      mJournal.getJournalFormatter().serialize(
          entry.toBuilder().setSequenceNumber(mNextEntrySequenceNumber ++).build(),
          mDataOutputStream);
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
      if (mIsClosed) {
        return;
      }
      mDataOutputStream.flush();
      if (mRawOutputStream instanceof FSDataOutputStream) {
        // The output stream directly created by {@link UnderFileSystem} may be
        // {@link FSDataOutputStream}, which means the under filesystem is HDFS, but
        // {@link DataOutputStream#flush} won't flush the data to HDFS, so we need to call
        // {@link FSDataOutputStream#sync} to actually flush data to HDFS.
        ((FSDataOutputStream) mRawOutputStream).sync();
      }
      boolean overSize = mDataOutputStream.size() > mMaxLogSize;
      if (overSize || mUfs.getUnderFSType() == UnderFileSystem.UnderFSType.S3) {
        // (1) The log file is oversize, needs to be rotated. Or
        // (2) Underfs is S3, flush on S3OutputStream will only flush to local temporary file,
        //     call close and complete the log to sync the journal entry to S3.
        if (overSize) {
          LOG.info("Rotating log file. size: {} maxSize: {}", mDataOutputStream.size(),
              mMaxLogSize);
        }
        // rotate the current log.
        mDataOutputStream.close();
        completeCurrentLog();
        mRawOutputStream = openCurrentLog();
        mDataOutputStream = new DataOutputStream(mRawOutputStream);
      }
    }
  }
}

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

package tachyon.master.next.journal;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
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
public class JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // TODO: make this a config parameter.
  private static final int MAX_LOG_SIZE = 10 * Constants.MB;

  private final Journal mJournal;
  private final TachyonConf mTachyonConf;
  /** Absolute path to the directory storing all of the journal data. */
  private final String mJournalDirectory;
  /** Absolute path to the directory storing all completed logs. */
  private final String mCompletedDirectory;
  /** Absolute path to the temporary checkpoint file. */
  private final String mTempCheckpointPath;
  private final UnderFileSystem mUfs;

  /** The log number to assign to the next complete log. */
  private int mNextCompleteLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;

  /** The output stream singleton for the checkpoint file. */
  private CheckpointOutputStream mCheckpointOutputStream = null;
  /** The output stream singleton for the entry log files. */
  private EntryOutputStream mEntryOutputStream = null;

  // TODO: start from the last known sequence number
  /** The sequence number for the next entry in the log. */
  private long mNextEntrySequenceNumber = 1;

  JournalWriter(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
    mJournalDirectory = mJournal.getDirectory();
    mCompletedDirectory = mJournal.getCompletedDirectory();
    mTempCheckpointPath = mJournal.getCheckpointFilePath() + ".tmp";
    mUfs = UnderFileSystem.get(mJournalDirectory, mTachyonConf);
  }

  /**
   * Returns an output stream for the journal checkpoint. The returned output stream is a singleton
   * for this writer.
   *
   * @return the output stream for the journal checkpoint.
   * @throws IOException
   */
  public synchronized JournalOutputStream getCheckpointOutputStream() throws IOException {
    if (mCheckpointOutputStream == null) {
      LOG.info("Creating tmp checkpoint file: " + mTempCheckpointPath);
      if (!mUfs.exists(mJournalDirectory)) {
        LOG.info("Creating journal folder: " + mJournalDirectory);
        mUfs.mkdirs(mJournalDirectory, true);
      }
      mCheckpointOutputStream =
          new CheckpointOutputStream(new DataOutputStream(mUfs.create(mTempCheckpointPath)));
    }
    return mCheckpointOutputStream;
  }

  /**
   * Returns an output stream for the journal entries. The returned output stream is a singleton for
   * this writer.
   *
   * @return the output stream for the journal entries.
   * @throws IOException
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
   * Returns the current log file output stream
   *
   * @return the output stream for the current log file
   * @throws IOException
   */
  private DataOutputStream openCurrentLog() throws IOException {
    String currentLogFile = mJournal.getCurrentLogFilePath();
    DataOutputStream dos = new DataOutputStream(mUfs.create(currentLogFile));
    LOG.info("Opened current log file: " + currentLogFile);
    return dos;
  }

  /**
   * Deletes all of the logs in the completed folder.
   *
   * @throws IOException
   */
  private void deleteCompletedLogs() throws IOException {
    LOG.info("Deleting all completed log files...");
    // Loop over all complete logs starting from the beginning.
    // TODO: should the deletes start from the end?
    int logNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;
    String logFilename = mJournal.getCompletedLogFilePath(logNumber);
    while (mUfs.exists(logFilename)) {
      LOG.info("Deleting completed log: " + logFilename);
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
   * @throws IOException
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
    LOG.info("Completed current log: " + currentLog + " to completed log: " + completedLog);

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
     * @param entry an entry to write to the journal checkpoint file.
     * @throws IOException
     */
    @Override
    public void writeEntry(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException("Cannot write entry after closing the stream.");
      }
      mJournal.getJournalFormatter().serialize(
          new SerializableJournalEntry(mNextEntrySequenceNumber ++, entry), mOutputStream);
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
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      if (mIsClosed) {
        return;
      }
      mOutputStream.flush();
      mOutputStream.close();

      LOG.info("Successfully created tmp checkpoint file: " + mTempCheckpointPath);
      mUfs.delete(mJournal.getCheckpointFilePath(), false);
      // TODO: the real checkpoint should not be overwritten here, but after all operations.
      mUfs.rename(mTempCheckpointPath, mJournal.getCheckpointFilePath());
      mUfs.delete(mTempCheckpointPath, false);
      LOG.info("Renamed checkpoint file " + mTempCheckpointPath + " to "
          + mJournal.getCheckpointFilePath());

      // The checkpoint already reflects the information in the completed logs.
      deleteCompletedLogs();

      // Consider the current log to be complete.
      completeCurrentLog();

      mIsClosed = true;
    }

    @Override
    public void flush() throws IOException {
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
    private DataOutputStream mOutputStream;
    private boolean mIsClosed = false;

    EntryOutputStream(DataOutputStream outputStream) {
      mOutputStream = outputStream;
    }

    @Override
    public void writeEntry(JournalEntry entry) throws IOException {
      if (mIsClosed) {
        throw new IOException("Cannot write entry after closing the stream.");
      }
      mJournal.getJournalFormatter().serialize(
          new SerializableJournalEntry(mNextEntrySequenceNumber ++, entry), mOutputStream);
    }

    @Override
    public void close() throws IOException {
      if (mIsClosed) {
        return;
      }
      if (mOutputStream != null) {
        // Close the current log file.
        mOutputStream.close();
      }

      mIsClosed = true;
    }

    @Override
    public void flush() throws IOException {
      if (mIsClosed) {
        return;
      }
      mOutputStream.flush();
      if (mOutputStream instanceof FSDataOutputStream) {
        ((FSDataOutputStream) mOutputStream).sync();
      }
      if (mOutputStream.size() > MAX_LOG_SIZE) {
        // rotate the current log.
        mOutputStream.close();
        completeCurrentLog();
        mOutputStream = openCurrentLog();
      }
    }
  }
}

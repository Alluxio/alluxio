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

public class JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // TODO: make this a config parameter.
  private static final int MAX_LOG_SIZE = 10 * Constants.MB;
  private static final int FIRST_COMPLETED_LOG_NUMBER = 1;

  private final Journal mJournal;
  private final TachyonConf mTachyonConf;
  /** Absolute path to the directory storing all of the journal data. */
  private final String mJournalDirectory;
  /** Absolute path to the directory storing all completed logs. */
  private final String mCompletedDirectory;
  private final UnderFileSystem mUfs;

  /** The log number to assign to the next complete log. */
  private int mNextCompleteLogNumber = FIRST_COMPLETED_LOG_NUMBER;

  /** The output stream for the current log. */
  private DataOutputStream mOutputStream = null;

  JournalWriter(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
    mJournalDirectory = mJournal.getDirectory();
    mCompletedDirectory = mJournal.getCompletedDirectory();
    mUfs = UnderFileSystem.get(mJournalDirectory, mTachyonConf);
  }

  /**
   * Write a checkpoint file. The checkpoint entry parameter should already reflect all the state of
   * the complete log files (but not the last, current log file).
   *
   * Writing the checkpoint file will delete all the completed log files, since the checkpoint
   * already reflects that state.
   *
   * The current log file (if it exists) will be closed and considered complete.
   *
   * @param entry the checkpoint journal entry to write.
   * @throws IOException
   */
  public void writeCheckpoint(JournalEntry entry) throws IOException {
    String checkpointPath = mJournal.getCheckpointFilePath();
    String tmpCheckpointPath = checkpointPath + ".tmp";
    LOG.info("Creating tmp checkpoint file: " + tmpCheckpointPath);
    if (!mUfs.exists(mJournalDirectory)) {
      LOG.info("Creating journal folder: " + mJournalDirectory);
      mUfs.mkdirs(mJournalDirectory, true);
    }
    DataOutputStream dos = new DataOutputStream(mUfs.create(tmpCheckpointPath));
    mJournal.getJournalFormatter().serialize(entry, dos);
    dos.flush();
    dos.close();

    LOG.info("Successfully created tmp checkpoint file: " + tmpCheckpointPath);
    mUfs.delete(checkpointPath, false);
    mUfs.rename(tmpCheckpointPath, checkpointPath);
    mUfs.delete(tmpCheckpointPath, false);
    LOG.info("Renamed checkpoint file " + tmpCheckpointPath + " to " + checkpointPath);
    // TODO: the real checkpoint should not be overwritten here, but after all operations.

    // The checkpoint already reflects the information in the completed logs.
    deleteCompletedLogs();

    // Consider the current log to be complete.
    completeCurrentLog();

    // Open the current log file, in preparation to write new entries.
    openCurrentLog();
  }

  public void writeEntry(JournalEntry entry) throws IOException {
    if (mOutputStream != null) {
      throw new IOException("The journal checkpoint must be written before writing entries.");
    }
    mJournal.getJournalFormatter().serialize(entry, mOutputStream);
  }

  public void flush() throws IOException {
    if (mOutputStream != null) {
      mOutputStream.flush();
      if (mOutputStream instanceof FSDataOutputStream) {
        ((FSDataOutputStream) mOutputStream).sync();
      }
      if (mOutputStream.size() > MAX_LOG_SIZE) {
        // rotate the current log.
        completeCurrentLog();
        openCurrentLog();
      }
    }
  }

  public void close() throws IOException {
    if (mOutputStream != null) {
      // Close the current log file.
      mOutputStream.close();
      mOutputStream = null;
    }
    // Close the ufs.
    mUfs.close();
  }

  private void openCurrentLog() throws IOException {
    String currentLogFile = mJournal.getCurrentLogFilePath();
    mOutputStream = new DataOutputStream(mUfs.create(currentLogFile));
    LOG.info("Created new current log file: " + currentLogFile);
  }

  private void deleteCompletedLogs() throws IOException {
    LOG.info("Deleting all completed log files");
    // Loop over all complete logs starting from the beginning.
    // TODO: should the deletes start from the end?
    int logNumber = FIRST_COMPLETED_LOG_NUMBER;
    String logFilename = mJournal.getCompletedLogFilePath(logNumber);
    while (mUfs.exists(logFilename)) {
      LOG.info("Deleting completed log: " + logFilename);
      mUfs.delete(logFilename, true);
      logNumber ++;
      // generate the next completed log filename in the sequence.
      logFilename = mJournal.getCompletedLogFilePath(logNumber);
    }

    // All complete logs are deleted. Reset the log number counter.
    mNextCompleteLogNumber = FIRST_COMPLETED_LOG_NUMBER;
  }

  private void completeCurrentLog() throws IOException {
    if (mOutputStream != null) {
      mOutputStream.close();
    }
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
}

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

import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class manages reading from the journal. The reading must occur in two phases:
 *
 * 1. First, the checkpoint file must be read.
 *
 * 2. Afterwards, completed entries are read in order. Only completed logs are read, so the last log
 * currently being written is not read until it is marked as complete.
 */
@NotThreadSafe
public class JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  /** Absolute path for the journal checkpoint file. */
  private final String mCheckpointPath;

  /** true if the checkpoint has already been read. */
  private boolean mCheckpointRead = false;
  /** The modified time (in ms) for the opened checkpoint file. */
  private long mCheckpointOpenedTime = -1;
  /** The modified time (in ms) for the latest checkpoint file. */
  private long mCheckpointLastModifiedTime = -1;
  /** The log number for the completed log file. */
  private long mCurrentLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;

  /**
   * Creates a new instance of {@link JournalReader}.
   *
   * @param journal the handle to the journal
   */
  JournalReader(Journal journal) {
    mJournal = Preconditions.checkNotNull(journal);
    mUfs = UnderFileSystem.get(mJournal.getDirectory());
    mCheckpointPath = mJournal.getCheckpointFilePath();
  }

  /**
   * Checks to see if the journal checkpoint has not been updated. If it has been updated since the
   * creation of this reader, this reader is no longer valid.
   *
   * @return true if the checkpoint file has not been modified
   */
  public boolean isValid() {
    return mCheckpointRead && (mCheckpointOpenedTime == mCheckpointLastModifiedTime);
  }

  /**
   * Gets the {@link JournalInputStream} for the journal checkpoint file. This must be called before
   * calling {@link #getNextInputStream()}.
   *
   * @return the {@link JournalInputStream} for the journal checkpoint file
   * @throws IOException if the checkpoint file cannot be read, or was already read
   */
  public JournalInputStream getCheckpointInputStream() throws IOException {
    if (mCheckpointRead) {
      throw new IOException("Checkpoint file has already been read.");
    }
    mCheckpointOpenedTime = getCheckpointLastModifiedTimeMs();

    LOG.info("Opening journal checkpoint file: {}", mCheckpointPath);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(mCheckpointPath));

    mCheckpointRead = true;
    return jis;
  }

  /**
   * @return the input stream for the next completed log file. Will return null if the next
   *         completed log file does not exist yet.
   * @throws IOException if the reader is no longer valid or when trying to get an input stream
   *                     before a checkpoint was read
   */
  public JournalInputStream getNextInputStream() throws IOException {
    if (!mCheckpointRead) {
      throw new IOException("Must read the checkpoint file before getting input stream.");
    }
    if (getCheckpointLastModifiedTimeMs() != mCheckpointOpenedTime) {
      throw new IOException("Checkpoint file has been updated. This reader is no longer valid.");
    }
    String currentLogPath = mJournal.getCompletedLogFilePath(mCurrentLogNumber);
    if (!mUfs.exists(currentLogPath)) {
      LOG.debug("Journal log file: {} does not exist yet.", currentLogPath);
      return null;
    }
    // Open input stream from the current log file.
    LOG.info("Opening journal log file: {}", currentLogPath);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(currentLogPath));

    // Increment the log file number.
    mCurrentLogNumber++;
    return jis;
  }

  /**
   * @return the last modified time of the checkpoint file in ms
   * @throws IOException if the checkpoint does not exist
   */
  public long getCheckpointLastModifiedTimeMs() throws IOException {
    if (!mUfs.exists(mCheckpointPath)) {
      throw new IOException("Checkpoint file " + mCheckpointPath + " does not exist.");
    }
    mCheckpointLastModifiedTime = mUfs.getModificationTimeMs(mCheckpointPath);
    return mCheckpointLastModifiedTime;
  }
}

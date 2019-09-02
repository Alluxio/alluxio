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
import alluxio.master.journalv0.JournalInputStream;
import alluxio.master.journalv0.JournalReader;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalReader} based on UFS.
 */
@NotThreadSafe
public class UfsJournalReader implements JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalReader.class);

  private final UfsJournal mJournal;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;
  /** Absolute path for the journal checkpoint. */
  private final URI mCheckpoint;

  /** true if the checkpoint has already been read. */
  private boolean mCheckpointRead = false;
  /** The modified time (in ms) for the opened checkpoint file. */
  private long mCheckpointOpenedTime = -1;
  /** The modified time (in ms) for the latest checkpoint file. */
  private long mCheckpointLastModifiedTime = -1;
  /** The log number for the completed log file. */
  private long mCurrentLogNumber = UfsJournal.FIRST_COMPLETED_LOG_NUMBER;

  /**
   * Creates a new instance of {@link UfsJournalReader}.
   *
   * @param journal the handle to the journal
   */
  UfsJournalReader(UfsJournal journal) {
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = UnderFileSystem.Factory.create(mJournal.getLocation().toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    mCheckpoint = mJournal.getCheckpoint();
  }

  @Override
  public boolean isValid() {
    return mCheckpointRead && (mCheckpointOpenedTime == mCheckpointLastModifiedTime);
  }

  @Override
  public JournalInputStream getCheckpointInputStream() throws IOException {
    if (mCheckpointRead) {
      throw new IOException("Checkpoint file has already been read.");
    }
    mCheckpointOpenedTime = getCheckpointLastModifiedTimeMs();

    LOG.info("Opening journal checkpoint file: {}", mCheckpoint);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(mCheckpoint.toString()));

    mCheckpointRead = true;
    return jis;
  }

  @Override
  public JournalInputStream getNextInputStream() throws IOException {
    if (!mCheckpointRead) {
      throw new IOException("Must read the checkpoint file before getting input stream.");
    }
    if (getCheckpointLastModifiedTimeMs() != mCheckpointOpenedTime) {
      throw new IOException("Checkpoint file has been updated. This reader is no longer valid.");
    }
    URI currentLog = mJournal.getCompletedLog(mCurrentLogNumber);
    if (!mUfs.isFile(currentLog.toString())) {
      LOG.debug("Journal log file: {} does not exist yet.", currentLog);
      return null;
    }
    // Open input stream from the current log file.
    LOG.info("Opening journal log file: {}", currentLog);
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(mUfs.open(currentLog.toString()));

    // Increment the log file number.
    mCurrentLogNumber++;
    return jis;
  }

  @Override
  public long getCheckpointLastModifiedTimeMs() throws IOException {
    if (!mUfs.isFile(mCheckpoint.toString())) {
      throw new IOException("Checkpoint file " + mCheckpoint + " does not exist.");
    }
    mCheckpointLastModifiedTime = mUfs.getFileStatus(mCheckpoint.toString()).getLastModifiedTime();
    return mCheckpointLastModifiedTime;
  }
}

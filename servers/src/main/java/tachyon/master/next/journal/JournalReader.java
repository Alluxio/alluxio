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

import java.io.DataInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

public class JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  private final TachyonConf mTachyonConf;
  private final UnderFileSystem mUfs;
  private final String mCheckpointPath;

  // true if the checkpoint has been already been read.
  private boolean mCheckpointRead = false;
  // The modified time (in ms) for the opened checkpoint file.
  private long mCheckpointOpenedTime = -1;
  // The modified time (in ms) for the latest checkpoint file.
  private long mLatestCheckpointModifiedTime = -1;
  private int mCurrentLogNumber = Journal.FIRST_COMPLETED_LOG_NUMBER;

  JournalReader(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
    mUfs = UnderFileSystem.get(mJournal.getDirectory(), mTachyonConf);
    mCheckpointPath = mJournal.getCheckpointFilePath();
  }

  /**
   * Checks to see if the journal checkpoint has not be updated. If it has been updated since the
   * creation of this reader, this reader is no longer valid.
   *
   * @return true if the checkpoint file has not been modified.
   */
  public boolean isValid() {
    if (!mCheckpointRead || (mCheckpointOpenedTime != mLatestCheckpointModifiedTime)) {
      return false;
    }
    return true;
  }

  public JournalEntry readCheckpoint() throws IOException {
    if (mCheckpointRead) {
      throw new IOException("Checkpoint file has already been read.");
    }
    mCheckpointOpenedTime = getCheckpointLastModifiedTime();

    DataInputStream inputStream = new DataInputStream(mUfs.open(mCheckpointPath));
    JournalInputStream jis = mJournal.getJournalFormatter().deserialize(inputStream);
    // TODO: should the checkpoint be multiple entries?
    JournalEntry checkpoint = jis.getNextEvent();

    jis.close();
    inputStream.close();

    mCheckpointRead = true;
    return checkpoint;
  }

  public JournalInputStream getNextInputStream() throws IOException {
    if (!mCheckpointRead) {
      throw new IOException("Must read the checkpoint file before getting input stream.");
    }
    if (getCheckpointLastModifiedTime() != mCheckpointOpenedTime) {
      throw new IOException("Checkpoint file has been updated. This reader is no longer valid.");
    }
    String currentLogPath = mJournal.getCompletedLogFilePath(mCurrentLogNumber);
    if (!mUfs.exists(currentLogPath)) {
      LOG.info("Journal log file: " + currentLogPath + " does not exist yet.");
      return null;
    }
    // Open input stream from the current log file.
    JournalInputStream jis =
        mJournal.getJournalFormatter().deserialize(new DataInputStream(mUfs.open(currentLogPath)));

    // Increment the log file number.
    mCurrentLogNumber ++;
    return jis;
  }

  private long getCheckpointLastModifiedTime() throws IOException {
    if (!mUfs.exists(mCheckpointPath)) {
      throw new IOException("Checkpoint file " + mCheckpointPath + " does not exist.");
    }
    mLatestCheckpointModifiedTime = mUfs.getModificationTimeMs(mCheckpointPath);
    return mLatestCheckpointModifiedTime;
  }
}

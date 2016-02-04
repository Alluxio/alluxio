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

package alluxio.master.journal;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.master.Master;
import alluxio.proto.journal.Journal.JournalEntry;

/**
 * This class tails the journal for a master. It will process the journal checkpoint file, and then
 * process all existing completed log files.
 */
@NotThreadSafe
public final class JournalTailer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The master to apply all the journal entries to. */
  private final Master mMaster;
  /** The journal to tail. */
  private final Journal mJournal;
  /** The journal reader to read journal entries. */
  private final JournalReader mReader;
  /** This keeps track of the latest sequence number seen in the journal entries. */
  private long mLatestSequenceNumber = 0;

  /**
   * Creates a new instance of {@link JournalTailer}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public JournalTailer(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mJournal = Preconditions.checkNotNull(journal);
    mReader = ((ReadOnlyJournal) mJournal).getNewReader();
  }

  /**
   * @return true if this tailer is valid, false otherwise
   */
  public boolean isValid() {
    return mReader.isValid();
  }

  /**
   * @return true if the checkpoint exists
   */
  public boolean checkpointExists() {
    try {
      mReader.getCheckpointLastModifiedTimeMs();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * @return the sequence number of the latest entry in the journal read so far
   */
  public long getLatestSequenceNumber() {
    return mLatestSequenceNumber;
  }

  /**
   * Loads and (optionally) processes the journal checkpoint file.
   *
   * @param applyToMaster if true, apply all the checkpoint events to the master. Otherwise, simply
   *        open the checkpoint file.
   * @throws IOException if an I/O error occurs
   */
  public void processJournalCheckpoint(boolean applyToMaster) throws IOException {
    // Load the checkpoint file.
    LOG.info("{}: Loading checkpoint file: {}", mMaster.getName(),
        mJournal.getCheckpointFilePath());
    // The checkpoint stream must be retrieved before retrieving any log file streams, because the
    // journal reader verifies that the checkpoint was read before the log files.
    JournalInputStream is = mReader.getCheckpointInputStream();

    if (applyToMaster) {
      // Only apply the checkpoint to the master, if specified.
      mMaster.processJournalCheckpoint(is);
    }
    // update the latest sequence number seen.
    mLatestSequenceNumber = is.getLatestSequenceNumber();
    is.close();
  }

  /**
   * Processes all the next completed journal log files. This method will return when the next
   * complete file is not found.
   *
   * {@link #processJournalCheckpoint(boolean)} must have been called previously.
   *
   * @return the number of completed log files processed
   * @throws IOException if an I/O error occurs
   */
  public int processNextJournalLogFiles() throws IOException {
    int numFilesProcessed = 0;
    while (mReader.isValid()) {
      // Process the new completed log file, if it exists.
      JournalInputStream inputStream = mReader.getNextInputStream();
      if (inputStream != null) {
        LOG.info("{}: Processing a completed log file.", mMaster.getName());
        JournalEntry entry;
        while ((entry = inputStream.getNextEntry()) != null) {
          mMaster.processJournalEntry(entry);
          // update the latest sequence number seen.
          mLatestSequenceNumber = inputStream.getLatestSequenceNumber();
        }
        inputStream.close();
        numFilesProcessed ++;
        LOG.info("{}: Finished processing the log file.", mMaster.getName());
      } else {
        return numFilesProcessed;
      }
    }
    LOG.info("{}: The checkpoint is out of date. Must reload checkpoint file.",
        mMaster.getName(), mJournal.getCheckpointFilePath());
    return numFilesProcessed;
  }
}

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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.Master;

public final class JournalTailer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Master mMaster;
  private final Journal mJournal;
  private final JournalReader mReader;

  public JournalTailer(Master master, Journal journal) {
    mMaster = master;
    mJournal = journal;
    mReader = mJournal.getNewReader();
  }

  /**
   * Returns whether or not the tailer is currently valid.
   *
   * @return true if this tailer is valid.
   */
  public boolean isValid() {
    return mReader.isValid();
  }

  /**
   * Returns the last modified time for the checkpoint file.
   *
   * @return the last modified time of the checkpoint file in ms.
   * @throws IOException
   */
  public long getCheckpointLastModifiedTime() throws IOException {
    return mReader.getCheckpointLastModifiedTime();
  }

  /**
   * Load and (optionally) process the journal checkpoint file.
   *
   * @param applyToMaster if true, apply all the checkpoint events to the master. Otherwise, simply
   *        open the checkpoint file.
   * @throws IOException
   */
  public void processJournalCheckpoint(boolean applyToMaster) throws IOException {
    // Load the checkpoint file.
    LOG.info("Loading checkpoint file: " + mJournal.getCheckpointFilePath());
    JournalInputStream is = mReader.getCheckpointInputStream();
    if (applyToMaster) {
      mMaster.processJournalCheckpoint(is);
    }
  }

  /**
   * Process all the next journal log files. {@link #processJournalCheckpoint(boolean)} must have
   * been called previously.
   *
   * @return the number of log files processed.
   * @throws IOException
   */
  public int processNextJournalLogFiles() throws IOException {
    int numProcessed = 0;
    while (true) {
      if (!mReader.isValid()) {
        LOG.info("The checkpoint is out of date. Must reload checkpoint file. "
            + mJournal.getCheckpointFilePath());
        return numProcessed;
      }

      // Process the new completed log file, if it exists.
      JournalInputStream inputStream = mReader.getNextInputStream();
      if (inputStream != null) {
        LOG.info("Processing a completed log file.");
        JournalEntry entry;
        while ((entry = inputStream.getNextEntry()) != null) {
          mMaster.processJournalEntry(entry);
        }
        inputStream.close();
        numProcessed ++;
        LOG.info("Finished processing the log file.");
      } else {
        return numProcessed;
      }
    }
  }
}

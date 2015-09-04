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

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * This encapsulates the journal for a master. The journal is made up of 2 components:
 * - The checkpoint: the full state of the master
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * To construct the full state of the master, all the entries must be applied to the checkpoint in
 * order. The entry file most recently being written to is in the base journal folder, where the
 * completed entry files are in the "completed/" sub-directory.
 */
public class Journal {
  public static final int FIRST_COMPLETED_LOG_NUMBER = 1;
  private static final String COMPLETED_DIRECTORY = "completed/";
  private static final String CURRENT_LOG_EXTENSION = ".out";

  // TODO: should this be a config parameter?
  /** The filename of the checkpoint file. */
  private final String mCheckpointFilename = "checkpoint.data";
  // TODO: should this be a config parameter?
  /** The base of the entry log filenames, without the file extension. */
  private final String mEntryLogFilenameBase = "log";
  private final String mDirectory;
  private final TachyonConf mTachyonConf;
  private final JournalFormatter mJournalFormatter;

  public Journal(String directory, TachyonConf tachyonConf) {
    if (!directory.endsWith(TachyonURI.SEPARATOR)) {
      // Ensure directory format.
      directory += TachyonURI.SEPARATOR;
    }
    mDirectory = directory;
    mTachyonConf = tachyonConf;
    // TODO: maybe this can be constructed, specified by a parameter in tachyonConf.
    mJournalFormatter = new JsonJournalFormatter();
  }

  public String getDirectory() {
    return mDirectory;
  }

  public String getCompletedDirectory() {
    return mDirectory + COMPLETED_DIRECTORY;
  }

  public String getCheckpointFilePath() {
    return mDirectory + mCheckpointFilename;
  }

  public String getCurrentLogFilePath() {
    return mDirectory + mEntryLogFilenameBase + CURRENT_LOG_EXTENSION;
  }

  /**
   * Returns the completed log filename for a particular log number.
   *
   * @param logNumber the log number to get the path for.
   * @return The absolute path of the completed log for a given log number.
   */
  public String getCompletedLogFilePath(int logNumber) {
    return getCompletedDirectory() + String.format("%s.%07d", mEntryLogFilenameBase, logNumber);
  }

  public JournalFormatter getJournalFormatter() {
    return mJournalFormatter;
  }

  public ReadOnlyJournal getReadOnlyJournal() {
    return new ReadOnlyJournal(mDirectory, mTachyonConf);
  }

  public JournalWriter getNewWriter() {
    return new JournalWriter(this, mTachyonConf);
  }

  public JournalReader getNewReader() {
    return new JournalReader(this, mTachyonConf);
  }
}

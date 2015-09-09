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

import com.google.common.base.Preconditions;

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
  /** The log number for the first completed log file. */
  public static final int FIRST_COMPLETED_LOG_NUMBER = 1;
  /** The directory for completed log files, relative to the base journal directory. */
  private static final String COMPLETED_DIRECTORY = "completed/";
  /** The file extension for the current log file. */
  private static final String CURRENT_LOG_EXTENSION = ".out";
  /** The filename of the checkpoint file. */
  private static final String CHECKPOINT_FILENAME = "checkpoint.data";
  /** The base of the entry log filenames, without the file extension. */
  private static final String ENTRY_LOG_FILENAME_BASE = "log";

  private final TachyonConf mTachyonConf;
  /** The directory where this journal is stored. */
  private final String mDirectory;
  /** The formatter for this journal. */
  private final JournalFormatter mJournalFormatter;

  /**
   * @param directory the base directory for this journal
   * @param tachyonConf the tachyon conf
   */
  public Journal(String directory, TachyonConf tachyonConf) {
    if (!directory.endsWith(TachyonURI.SEPARATOR)) {
      // Ensure directory format.
      directory += TachyonURI.SEPARATOR;
    }
    mDirectory = directory;
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    // TODO: maybe this can be constructed, specified by a parameter in tachyonConf.
    mJournalFormatter = new JsonJournalFormatter();
  }

  /**
   * @return the base directory for this journal
   */
  public String getDirectory() {
    return mDirectory;
  }

  /**
   * @return the directory for where the completed log files are stored
   */
  public String getCompletedDirectory() {
    return mDirectory + COMPLETED_DIRECTORY;
  }

  /**
   * @return the absolute path for the journal checkpoint file
   */
  public String getCheckpointFilePath() {
    return mDirectory + CHECKPOINT_FILENAME;
  }

  /**
   * @return the absolute path for the current log file.
   */
  public String getCurrentLogFilePath() {
    return mDirectory + ENTRY_LOG_FILENAME_BASE + CURRENT_LOG_EXTENSION;
  }

  /**
   * Returns the completed log filename for a particular log number.
   *
   * @param logNumber the log number to get the path for.
   * @return The absolute path of the completed log for a given log number.
   */
  public String getCompletedLogFilePath(int logNumber) {
    return getCompletedDirectory() + String.format("%s.%07d", ENTRY_LOG_FILENAME_BASE, logNumber);
  }

  /**
   * @return the formatter for this journal
   */
  public JournalFormatter getJournalFormatter() {
    return mJournalFormatter;
  }

  /**
   * @return a readonly version of this journal
   */
  public ReadOnlyJournal getReadOnlyJournal() {
    return new ReadOnlyJournal(mDirectory, mTachyonConf);
  }

  /**
   * @return the writer for this journal
   */
  public JournalWriter getNewWriter() {
    return new JournalWriter(this, mTachyonConf);
  }

  /**
   * @return the reader for this journal
   */
  public JournalReader getNewReader() {
    return new JournalReader(this, mTachyonConf);
  }
}

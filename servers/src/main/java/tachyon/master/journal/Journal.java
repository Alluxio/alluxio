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
import tachyon.master.MasterContext;

/**
 * This class encapsulates the journal for a master. The journal is made up of 2 components:
 * - The checkpoint: the full state of the master
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * To construct the full state of the master, all the entries must be applied to the checkpoint in
 * order. The entry file most recently being written to is in the base journal folder, where the
 * completed entry files are in the "completed/" sub-directory.
 */
public abstract class Journal {
  /** The log number for the first completed log file. */
  public static final long FIRST_COMPLETED_LOG_NUMBER = 1L;
  /** The directory for completed log files, relative to the base journal directory. */
  private static final String COMPLETED_DIRECTORY = "completed/";
  /** The file extension for the current log file. */
  private static final String CURRENT_LOG_EXTENSION = ".out";
  /** The filename of the checkpoint file. */
  private static final String CHECKPOINT_FILENAME = "checkpoint.data";
  /** The base of the entry log filenames, without the file extension. */
  private static final String ENTRY_LOG_FILENAME_BASE = "log";
  /** The directory where this journal is stored. */
  private final String mDirectory;
  /** The formatter for this journal. */
  private final JournalFormatter mJournalFormatter;

  /**
   * Creates a new instance of {@link Journal}.
   *
   * @param directory the base directory for this journal
   */
  public Journal(String directory) {
    if (!directory.endsWith(TachyonURI.SEPARATOR)) {
      // Ensure directory format.
      directory += TachyonURI.SEPARATOR;
    }
    mDirectory = directory;
    mJournalFormatter = JournalFormatter.Factory.create(MasterContext.getConf());
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
   * @return the absolute path for the current log file
   */
  public String getCurrentLogFilePath() {
    return mDirectory + ENTRY_LOG_FILENAME_BASE + CURRENT_LOG_EXTENSION;
  }

  /**
   * Returns the completed log filename for a particular log number.
   *
   * @param logNumber the log number to get the path for
   * @return The absolute path of the completed log for a given log number
   */
  public String getCompletedLogFilePath(long logNumber) {
    return getCompletedDirectory() + String.format("%s.%020d", ENTRY_LOG_FILENAME_BASE, logNumber);
  }

  /**
   * @return the {@link JournalFormatter} for this journal
   */
  public JournalFormatter getJournalFormatter() {
    return mJournalFormatter;
  }
}

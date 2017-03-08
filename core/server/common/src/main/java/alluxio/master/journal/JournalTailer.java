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

import alluxio.master.Master;
import alluxio.master.journal.ufs.UfsJournalTailer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class tails the journal for a master. It will process the journal checkpoint, and then
 * process all existing completed logs.
 */
@NotThreadSafe
public interface JournalTailer {

  /**
   * Factory for {@link JournalTailer}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Creates a new instance of {@link JournalTailer}.
     *
     * @param master a master
     * @param journal a journal
     * @return a new {@link JournalTailer} instnace
     */
    public static JournalTailer create(Master master, Journal journal) {
      return new UfsJournalTailer(master, journal);
    }
  }

  /**
   * @return true if this tailer is valid, false otherwise
   */
  boolean isValid();

  /**
   * @return true if the checkpoint exists
   */
  boolean checkpointExists();

  /**
   * @return the sequence number of the latest entry in the journal read so far
   */
  long getLatestSequenceNumber();

  /**
   * Loads and (optionally) processes the journal checkpoint.
   *
   * @param applyToMaster if true, apply all the checkpoint events to the master. Otherwise, simply
   *        open the checkpoint.
   * @throws IOException if an I/O error occurs
   */
  void processJournalCheckpoint(boolean applyToMaster) throws IOException;

  /**
   * Processes all the completed journal logs. This method will return when it processes the last
   * completed log.
   *
   * {@link #processJournalCheckpoint(boolean)} must have been called previously.
   *
   * @return the number of logs processed
   * @throws IOException if an I/O error occurs
   */
  int processNextJournalLogs() throws IOException;
}

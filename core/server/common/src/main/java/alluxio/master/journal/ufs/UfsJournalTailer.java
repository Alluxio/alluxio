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

package alluxio.master.journal.ufs;

import alluxio.master.Master;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalInputStream;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalTailer;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link JournalTailer} based on UFS.
 */
@NotThreadSafe
public final class UfsJournalTailer implements JournalTailer {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalTailer.class);

  /** The master to apply all the journal entries to. */
  private final Master mMaster;
  /** The journal reader to read journal entries. */
  private final JournalReader mReader;
  /** This keeps track of the latest sequence number seen in the journal entries. */
  private long mLatestSequenceNumber = 0;

  /**
   * Creates a new instance of {@link UfsJournalTailer}.
   *
   * @param master the master to apply the journal entries to
   * @param journal the journal to tail
   */
  public UfsJournalTailer(Master master, Journal journal) {
    mMaster = Preconditions.checkNotNull(master);
    mReader = journal.getReader();
  }

  @Override
  public boolean isValid() {
    return mReader.isValid();
  }

  @Override
  public boolean checkpointExists() {
    try {
      mReader.getCheckpointLastModifiedTimeMs();
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to check if checkpoint exists: {}", e.getMessage());
      LOG.debug("Exception: ", e);
      return false;
    }
  }

  @Override
  public long getLatestSequenceNumber() {
    return mLatestSequenceNumber;
  }

  @Override
  public void processJournalCheckpoint(boolean applyToMaster) throws IOException {
    // Load the checkpoint.
    LOG.info("{}: Loading checkpoint.", mMaster.getName());
    // The checkpoint stream must be retrieved before retrieving any log streams, because the
    // journal reader verifies that the checkpoint was read before the log streams.
    try (JournalInputStream is = mReader.getCheckpointInputStream()) {
      if (applyToMaster) {
        // Only apply the checkpoint to the master, if specified.
        mMaster.processJournalCheckpoint(is);
      }
      // update the latest sequence number seen.
      mLatestSequenceNumber = is.getLatestSequenceNumber();
    }
  }

  @Override
  public int processNextJournalLogs() throws IOException {
    int numFilesProcessed = 0;
    while (mReader.isValid()) {
      // Process the new completed log, if it exists.
      JournalInputStream inputStream = mReader.getNextInputStream();
      if (inputStream != null) {
        LOG.info("{}: Processing a completed log.", mMaster.getName());
        JournalEntry entry;
        while ((entry = inputStream.read()) != null) {
          mMaster.processJournalEntry(entry);
          // update the latest sequence number seen.
          mLatestSequenceNumber = inputStream.getLatestSequenceNumber();
        }
        inputStream.close();
        numFilesProcessed++;
        LOG.info("{}: Finished processing the log.", mMaster.getName());
      } else {
        return numFilesProcessed;
      }
    }
    LOG.info("{}: The checkpoint is out of date and must be reloaded.", mMaster.getName());
    return numFilesProcessed;
  }
}

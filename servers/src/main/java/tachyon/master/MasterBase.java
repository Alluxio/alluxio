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

package tachyon.master;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalInputStream;
import tachyon.master.journal.JournalSerializable;
import tachyon.master.journal.JournalTailer;
import tachyon.master.journal.JournalTailerThread;
import tachyon.master.journal.JournalWriter;

public abstract class MasterBase implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  private final ExecutorService mExecutorService;

  /** true if this master is in leader mode, and not standby mode. */
  private boolean mIsLeader = false;

  /** The thread that tails the journal when the master is in standby mode. */
  private JournalTailerThread mStandbyJournalTailer = null;

  private JournalWriter mJournalWriter = null;

  protected MasterBase(Journal journal, ExecutorService executorService) {
    mJournal = Preconditions.checkNotNull(journal);
    mExecutorService = Preconditions.checkNotNull(executorService);
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    JournalEntry entry;
    while ((entry = inputStream.getNextEntry()) != null) {
      processJournalEntry(entry);
    }
    inputStream.close();
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    LOG.info("Starting master. isLeader: " + isLeader);
    mIsLeader = isLeader;
    if (mIsLeader) {
      // Replay all the state of the checkpoint and the completed log files.
      // TODO: only do this if this is a fresh start, not if this master had already been tailing
      // the journal.
      LOG.info(getProcessorName() + ": process completed logs before becoming master.");
      JournalTailer catchupTailer = new JournalTailer(this, mJournal);
      boolean checkpointExists = true;
      try {
        catchupTailer.getCheckpointLastModifiedTimeMs();
      } catch (IOException ioe) {
        // The checkpoint doesn't exist yet. This is probably the first execution ever, or this is a
        // testing master.
        checkpointExists = false;
      }
      if (checkpointExists) {
        catchupTailer.processJournalCheckpoint(true);
        catchupTailer.processNextJournalLogFiles();
      }

      // initialize the journal and write out the checkpoint file (the state of all completed logs).
      mJournalWriter = mJournal.getNewWriter();
      writeToJournal(mJournalWriter.getCheckpointOutputStream());
      mJournalWriter.getCheckpointOutputStream().close();

      // Final catchup stage. The last in-progress file (if it existed) was marked as complete when
      // the checkpoint file was closed. That last completed file must be processed to get to the
      // latest state. Read and process the completed file.
      LOG.info(getProcessorName() + ": process the last completed log before becoming master.");
      catchupTailer = new JournalTailer(this, mJournal);
      catchupTailer.processJournalCheckpoint(false);
      catchupTailer.processNextJournalLogFiles();
    } else {
      // in standby mode. Start the journal tailer thread.
      mStandbyJournalTailer = new JournalTailerThread(this, mJournal);
      mStandbyJournalTailer.start();
    }
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping master. isLeader: " + isLeaderMode());
    if (isStandbyMode()) {
      if (mStandbyJournalTailer != null) {
        // stop and wait for the journal tailer thread.
        mStandbyJournalTailer.shutdownAndJoin();
      }
    } else {
      // Stop this master.
      if (mJournalWriter != null) {
        mJournalWriter.close();
        mJournalWriter = null;
      }
    }
  }

  protected boolean isLeaderMode() {
    return mIsLeader;
  }

  protected boolean isStandbyMode() {
    return !mIsLeader;
  }

  protected void writeJournalEntry(JournalEntry entry) {
    if (mJournalWriter == null) {
      throw new RuntimeException("Cannot write entry: journal writer is null.");
    }
    try {
      mJournalWriter.getEntryOutputStream().writeEntry(entry);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void writeJournalEntry(JournalSerializable entry) {
    if (mJournalWriter == null) {
      throw new RuntimeException("Cannot write entry: journal writer is null.");
    }
    try {
      entry.writeToJournal(mJournalWriter.getEntryOutputStream());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void flushJournal() {
    if (mJournalWriter == null) {
      throw new RuntimeException("Cannot flush journal: Journal writer is null.");
    }
    try {
      mJournalWriter.getEntryOutputStream().flush();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected ExecutorService getExecutorService() {
    return mExecutorService;
  }
}

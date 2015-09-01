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

package tachyon.master.next;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalSerializable;
import tachyon.master.next.journal.JournalTailerThread;
import tachyon.master.next.journal.JournalWriter;

public abstract class MasterBase implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;

  // true if this master is in standby mode.
  private boolean mIsStandbyMode = false;

  // The thread that tails the journal when the master is in standby mode.
  private JournalTailerThread mStandbyJournalTailer = null;

  private JournalWriter mJournalWriter = null;

  protected MasterBase(Journal journal) {
    mJournal = Preconditions.checkNotNull(journal);
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    JournalEntry entry;
    while ((entry = inputStream.getNextEntry()) != null) {
      processJournalEntry(entry);
    }
    inputStream.close();
  }

  protected boolean isMasterMode() {
    return !mIsStandbyMode;
  }

  protected boolean isStandbyMode() {
    return mIsStandbyMode;
  }

  protected void startMaster(boolean asMaster) throws IOException {
    LOG.info("Starting master. asMaster: " + asMaster);
    mIsStandbyMode = !asMaster;
    if (asMaster) {
      // initialize the journal and write out the checkpoint file.

      // TODO: only do this if this is a fresh start, not if this master had already been tailing
      // the journal.
      // Use the journal tailer to "catch up".
      LOG.info(getProcessorName() + ": start journal tailer to catch up before becoming master.");
      mStandbyJournalTailer = new JournalTailerThread(this, mJournal);
      mStandbyJournalTailer.start();
      mStandbyJournalTailer.shutdownAndJoin();

      // TODO: verify that journal writer is null?
      mJournalWriter = mJournal.getNewWriter();
      writeToJournal(mJournalWriter.getCheckpointOutputStream());
      mJournalWriter.getCheckpointOutputStream().close();
    } else {
      // in standby mode. Start the journal tailer thread.
      // TODO: verify that journal tailer is null?
      mStandbyJournalTailer = new JournalTailerThread(this, mJournal);
      mStandbyJournalTailer.start();
    }
  }

  protected void stopMaster() throws IOException {
    LOG.info("Stopping master. isMaster: " + isMasterMode());
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

  protected void writeJournalEntry(JournalEntry entry) {
    if (mJournalWriter == null) {
      // TODO: Add this check back
      // throw new RuntimeException("Cannot write entry: journal writer is null.");
      return;
    }
    try {
      mJournalWriter.getEntryOutputStream().writeEntry(entry);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void writeJournalEntry(JournalSerializable entry) {
    if (mJournalWriter == null) {
      // TODO: Add this check back
      // throw new RuntimeException("Cannot write entry: journal writer is null.");
      return;
    }
    try {
      entry.writeToJournal(mJournalWriter.getEntryOutputStream());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void flushJournal() {
    if (mJournalWriter == null) {
      // TODO: Add this check back
      // throw new RuntimeException("Cannot flush journal: Journal writer is null.");
      return;
    }
    try {
      mJournalWriter.getEntryOutputStream().flush();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}

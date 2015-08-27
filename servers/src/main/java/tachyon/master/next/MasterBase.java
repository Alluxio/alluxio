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

import tachyon.Constants;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
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
    mJournal = journal;
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
      mJournalWriter = mJournal.getNewWriter();
      writeJournalCheckpoint(mJournalWriter);
      mJournalWriter.closeCheckpoint();
    } else {
      // in standby mode. Start the journal tailer thread.
      mStandbyJournalTailer = new JournalTailerThread(this, mJournal);
      mStandbyJournalTailer.start();
    }
  }

  protected void stopMaster() throws IOException {
    LOG.info("Stopping master. isMaster: " + isMasterMode());
    if (isStandbyMode()) {
      // stop and wait for the journal tailer thread.
      mStandbyJournalTailer.shutdownAndJoin();
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
      throw new RuntimeException("Cannot write entry: journal writer is null.");
    }
    try {
      mJournalWriter.writeEntry(entry);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void flushJournal() {
    if (mJournalWriter == null) {
      throw new RuntimeException("Cannot flush journal: Journal writer is null.");
    }
    try {
      mJournalWriter.flush();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}

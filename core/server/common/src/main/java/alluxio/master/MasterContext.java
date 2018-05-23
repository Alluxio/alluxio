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

package alluxio.master;

import alluxio.master.journal.JournalSystem;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Lock;

/**
 * Stores context information for Alluxio masters.
 */
public final class MasterContext {
  private final JournalSystem mJournalSystem;
  private final SafeModeManager mSafeModeManager;
  private final Lock mStateLock;

  /**
   * @param journalSystem the journal system to use for tracking master operations
   * @param safeModeManager the manager for master safe mode
   * @param stateLock a lock which must be taken before modifying persistent (journaled) state
   */
  public MasterContext(JournalSystem journalSystem, SafeModeManager safeModeManager, Lock stateLock) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mSafeModeManager = Preconditions.checkNotNull(safeModeManager, "safeModeManager");
    mStateLock = Preconditions.checkNotNull(stateLock, "stateLock");
  }

  /**
   * @return the journal system to use for tracking master operations
   */
  public JournalSystem getJournalSystem() {
    return mJournalSystem;
  }

  /**
   * @return the manager for master safe mode
   */
  public SafeModeManager getSafeModeManager() {
    return mSafeModeManager;
  }

  /**
   * @return the state lock
   */
  public Lock getStateLock() {
    return mStateLock;
  }
}

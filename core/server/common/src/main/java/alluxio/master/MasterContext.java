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
import alluxio.security.user.ServerUserState;
import alluxio.security.user.UserState;
import alluxio.underfs.UfsManager;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Stores context information for Alluxio masters.
 *
 * @param <T> the type of ufsManager to be used
 */
public class MasterContext<T extends UfsManager> {
  private final JournalSystem mJournalSystem;
  private final PrimarySelector mPrimarySelector;
  /**
   * The stateLockManager is used to allow us to pause master state changes so that we can
   * take backups of master state. All state modifications should hold the lock in shared mode
   * so that holding it exclusively allows a thread to pause state modifications.
   */
  private final StateLockManager mStateLockManager;
  private final UserState mUserState;
  private final T mUfsManager;

  /**
   * Creates a new master context, using the global server UserState.
   *
   * @param journalSystem the journal system to use for tracking master operations
   * @param primarySelector the primary selector for selecting the leading master
   * @param ufsManager the UFS manager
   */
  public MasterContext(JournalSystem journalSystem, PrimarySelector primarySelector, T ufsManager) {
    this(journalSystem, primarySelector, null, ufsManager);
  }

  /**
   * Creates a new master context.
   *
   * @param journalSystem the journal system to use for tracking master operations
   * @param primarySelector the primary selector for selecting the leading master
   * @param userState the user state of the server. If null, will use the global server user state
   * @param ufsManager the UFS manager
   */
  public MasterContext(JournalSystem journalSystem, PrimarySelector primarySelector,
      @Nullable UserState userState, T ufsManager) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mPrimarySelector = Preconditions.checkNotNull(primarySelector, "primarySelector");
    mUfsManager = Preconditions.checkNotNull(ufsManager, "ufsManager");
    if (userState == null) {
      mUserState = ServerUserState.global();
    } else {
      mUserState = userState;
    }
    mStateLockManager = new StateLockManager();
  }

  /**
   * @return the journal system to use for tracking master operations
   */
  public JournalSystem getJournalSystem() {
    return mJournalSystem;
  }

  /**
   * @return the primary selector that selects the leading master
   */
  public PrimarySelector getPrimarySelector() {
    return mPrimarySelector;
  }

  /**
   * @return the UserState of the server
   */
  public UserState getUserState() {
    return mUserState;
  }

  /**
   * @return the state lock manager
   */
  public StateLockManager getStateLockManager() {
    return mStateLockManager;
  }

  /**
   * @return the ufs manager
   */
  public T getUfsManager() {
    return mUfsManager;
  }
}

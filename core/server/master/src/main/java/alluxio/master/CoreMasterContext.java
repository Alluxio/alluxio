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

/**
 * This class stores fields that are specific to core masters.
 */
public class CoreMasterContext extends MasterContext {
  private final SafeModeManager mSafeModeManager;
  private final BackupManager mBackupManager;
  private final long mStartTimeMs;
  private final int mPort;

  /**
   * @param journalSystem journal system
   * @param safeModeManager safe mode manager
   * @param backupManager backup manager
   * @param startTimeMs start time
   * @param port rpc port
   */
  public CoreMasterContext(JournalSystem journalSystem, SafeModeManager safeModeManager,
      BackupManager backupManager, long startTimeMs, int port) {
    super(journalSystem);

    mSafeModeManager = Preconditions.checkNotNull(safeModeManager, "safeModeManager");
    mBackupManager = Preconditions.checkNotNull(backupManager, "backupManager");
    mStartTimeMs = startTimeMs;
    mPort = port;
  }

  /**
   * @return the manager for master safe mode
   */
  public SafeModeManager getSafeModeManager() {
    return mSafeModeManager;
  }

  /**
   * @return the backup manager
   */
  public BackupManager getBackupManager() {
    return mBackupManager;
  }

  /**
   * @return the master process start time in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the rpc port
   */
  public int getPort() {
    return mPort;
  }
}

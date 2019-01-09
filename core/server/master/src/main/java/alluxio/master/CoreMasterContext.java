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

  private CoreMasterContext(Builder builder) {
    super(builder.mJournalSystem);

    mSafeModeManager = Preconditions.checkNotNull(builder.mSafeModeManager, "safeModeManager");
    mBackupManager = Preconditions.checkNotNull(builder.mBackupManager, "backupManager");
    mStartTimeMs = builder.mStartTimeMs;
    mPort = builder.mPort;
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

  /**
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Constructs {@link CoreMasterContext}s.
   */
  public static class Builder {
    private JournalSystem mJournalSystem;
    private SafeModeManager mSafeModeManager;
    private BackupManager mBackupManager;
    private long mStartTimeMs;
    private int mPort;

    /**
     * @param journalSystem journal system
     * @return the builder
     */
    public Builder setJournalSystem(JournalSystem journalSystem) {
      mJournalSystem = journalSystem;
      return this;
    }

    /**
     * @param safeModeManager safe mode manager
     * @return the builder
     */
    public Builder setSafeModeManager(SafeModeManager safeModeManager) {
      mSafeModeManager = safeModeManager;
      return this;
    }

    /**
     * @param backupManager backup manager
     * @return the builder
     */
    public Builder setBackupManager(BackupManager backupManager) {
      mBackupManager = backupManager;
      return this;
    }

    /**
     * @param startTimeMs start time in milliseconds
     * @return the builder
     */
    public Builder setStartTimeMs(long startTimeMs) {
      mStartTimeMs = startTimeMs;
      return this;
    }

    /**
     * @param port port
     * @return the builder
     */
    public Builder setPort(int port) {
      mPort = port;
      return this;
    }

    /**
     * @return the built CoreMasterContext
     */
    public CoreMasterContext build() {
      return new CoreMasterContext(this);
    }
  }
}

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
import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.InodeStore;
import alluxio.security.user.UserState;
import alluxio.underfs.MasterUfsManager;

import com.google.common.base.Preconditions;

/**
 * This class stores fields that are specific to core masters.
 */
public class CoreMasterContext extends MasterContext<MasterUfsManager> {
  private final SafeModeManager mSafeModeManager;
  private final BackupManager mBackupManager;
  private final BlockStore.Factory mBlockStoreFactory;
  private final InodeStore.Factory mInodeStoreFactory;
  private final JournalSystem mJournalSystem;
  private final long mStartTimeMs;
  private final int mPort;

  private CoreMasterContext(Builder builder) {
    super(builder.mJournalSystem, builder.mUserState, builder.mUfsManager);

    mSafeModeManager = Preconditions.checkNotNull(builder.mSafeModeManager, "safeModeManager");
    mBackupManager = Preconditions.checkNotNull(builder.mBackupManager, "backupManager");
    mBlockStoreFactory =
        Preconditions.checkNotNull(builder.mBlockStoreFactory, "blockStoreFactory");
    mInodeStoreFactory =
        Preconditions.checkNotNull(builder.mInodeStoreFactory, "inodeStoreFactory");
    mJournalSystem = Preconditions.checkNotNull(builder.mJournalSystem, "journalSystem");
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
   * @return the block store factory
   */
  public BlockStore.Factory getBlockStoreFactory() {
    return mBlockStoreFactory;
  }

  /**
   * @return the inode store factory
   */
  public InodeStore.Factory getInodeStoreFactory() {
    return mInodeStoreFactory;
  }

  /**
   * @return the journal system
   */
  public JournalSystem getJournalSystem() {
    return mJournalSystem;
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
    private UserState mUserState;
    private SafeModeManager mSafeModeManager;
    private BackupManager mBackupManager;
    private BlockStore.Factory mBlockStoreFactory;
    private InodeStore.Factory mInodeStoreFactory;
    private MasterUfsManager mUfsManager;
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
     * @param userState the user state
     * @return the builder
     */
    public Builder setUserState(UserState userState) {
      mUserState = userState;
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
     * @param blockStoreFactory factory for creating a block store
     * @return the builder
     */
    public Builder setBlockStoreFactory(BlockStore.Factory blockStoreFactory) {
      mBlockStoreFactory = blockStoreFactory;
      return this;
    }

    /**
     * @param inodeStoreFactory factory for creating an inode store
     * @return the builder
     */
    public Builder setInodeStoreFactory(InodeStore.Factory inodeStoreFactory) {
      mInodeStoreFactory = inodeStoreFactory;
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
     * @param ufsManager ufsManager
     * @return the builder
     */
    public Builder setUfsManager(MasterUfsManager ufsManager) {
      mUfsManager = ufsManager;
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

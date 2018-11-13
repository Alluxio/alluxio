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
import alluxio.master.metastore.Metastore;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Stores context information for Alluxio masters.
 */
public final class MasterContext {
  private final JournalSystem mJournalSystem;
  private final SafeModeManager mSafeModeManager;
  private final BackupManager mBackupManager;
  private final Metastore mMetastore;
  private final ReadWriteLock mStateLock;
  private final long mStartTimeMs;
  private final int mPort;

  /**
   * Creates a new master context.
   *
   * The stateLock is used to allow us to pause master state changes so that we can take backups of
   * master state. All state modifications should hold the read lock so that holding the write lock
   * allows a thread to pause state modifications.
   *
   * @param journalSystem the journal system to use for tracking master operations
   * @param safeModeManager the manager for master safe mode
   * @param backupManager the backup manager for performing backups
   * @param metastore the metastore for managing master metadata
   * @param startTimeMs the master process start time in milliseconds
   * @param port the rpc port
   */
  public MasterContext(JournalSystem journalSystem, SafeModeManager safeModeManager,
      BackupManager backupManager, Metastore metastore, long startTimeMs, int port) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mSafeModeManager = Preconditions.checkNotNull(safeModeManager, "safeModeManager");
    mBackupManager = Preconditions.checkNotNull(backupManager, "backupManager");
    mMetastore = metastore;
    mStateLock = new ReentrantReadWriteLock();
    mStartTimeMs = startTimeMs;
    mPort = port;
  }

  /**
   * Create a master context to be used for job masters.
   *
   * @param journalSystem the journal system to use for tracking master operations
   */
  public MasterContext(JournalSystem journalSystem) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mSafeModeManager = null;
    mBackupManager = null;
    mMetastore = null;
    mStateLock = new ReentrantReadWriteLock();
    mStartTimeMs = -1;
    mPort = -1;
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
   * @return the backup manager
   */
  public BackupManager getBackupManager() {
    return mBackupManager;
  }

  /**
   * @return the metastore
   */
  public Metastore getMetastore() {
    return mMetastore;
  }

  /**
   * @return the lock which must be held to modify master state
   */
  public Lock stateChangeLock() {
    return mStateLock.readLock();
  }

  /**
   * @return the lock which prevents master state from changing
   */
  public Lock pauseStateLock() {
    return mStateLock.writeLock();
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

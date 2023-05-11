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
import alluxio.util.executor.ExecutorServiceFactory;

import java.time.Clock;

/**
 * Abstract class for masters that run in the core master process. This class stores fields that
 * are specific to core masters.
 */
public abstract class CoreMaster extends AbstractMaster {
  protected final SafeModeManager mSafeModeManager;
  protected final BackupManager mBackupManager;
  protected final JournalSystem mJournalSystem;
  protected final long mStartTimeMs;
  protected final int mPort;

  /**
   * @param context the context for Alluxio master
   * @param clock the Clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for
   */
  protected CoreMaster(CoreMasterContext context, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(context, clock, executorServiceFactory);
    mSafeModeManager = context.getSafeModeManager();
    mBackupManager = context.getBackupManager();
    mJournalSystem = context.getJournalSystem();
    mStartTimeMs = context.getStartTimeMs();
    mPort = context.getPort();
  }
}

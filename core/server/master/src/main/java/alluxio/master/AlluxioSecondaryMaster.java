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

import alluxio.Process;
import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.underfs.MasterUfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.CommonUtils.ProcessType;
import alluxio.util.WaitForOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secondary Alluxio master that replays journal logs and writes checkpoints.
 */
@NotThreadSafe
public final class AlluxioSecondaryMaster implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSecondaryMaster.class);
  private final MasterRegistry mRegistry;
  private final JournalSystem mJournalSystem;
  private final CountDownLatch mLatch;
  private final SafeModeManager mSafeModeManager;
  private final BackupManager mBackupManager;
  private final long mStartTimeMs;
  private final int mPort;

  private volatile boolean mRunning = false;

  /**
   * Creates a {@link AlluxioSecondaryMaster}.
   */
  AlluxioSecondaryMaster() {
    try {
      URI journalLocation = JournalUtils.getJournalLocation();
      mJournalSystem = new JournalSystem.Builder()
          .setLocation(journalLocation).build(ProcessType.MASTER);
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      mBackupManager = new BackupManager(mRegistry);
      mStartTimeMs = System.currentTimeMillis();
      mPort = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_PORT);
      String baseDir = ServerConfiguration.get(PropertyKey.SECONDARY_MASTER_METASTORE_DIR);
      // Create masters.
      MasterUtils.createMasters(mRegistry, CoreMasterContext.newBuilder()
          .setJournalSystem(mJournalSystem)
          .setSafeModeManager(mSafeModeManager)
          .setBackupManager(mBackupManager)
          .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
          .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
          .setStartTimeMs(mStartTimeMs)
          .setPort(mPort)
          .setUfsManager(new MasterUfsManager())
          .build());
      // Check that journals of each service have been formatted.
      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", journalLocation));
      }
      mLatch = new CountDownLatch(1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    mRunning = true;
    mLatch.await();
    mJournalSystem.stop();
    mRegistry.close();
    mRunning = false;
  }

  @Override
  public void stop() throws Exception {
    mLatch.countDown();
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor("Secondary master to start", () -> mRunning,
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return "Alluxio secondary master";
  }

  /**
   * Starts the secondary Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  // TODO(peis): Move the non-static methods into AlluxioSecondaryMasterProcess for consistency.
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioSecondaryMaster.class.getCanonicalName());
      System.exit(-1);
    }

    AlluxioSecondaryMaster master = new AlluxioSecondaryMaster();
    ProcessUtils.run(master);
  }
}

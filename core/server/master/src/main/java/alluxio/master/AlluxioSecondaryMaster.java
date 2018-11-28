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

import alluxio.Configuration;
import alluxio.Process;
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

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

  /**
   * Creates a {@link AlluxioSecondaryMaster}.
   */
  AlluxioSecondaryMaster() {
    try {
      URI journalLocation = JournalUtils.getJournalLocation();
      mJournalSystem = new JournalSystem.Builder().setLocation(journalLocation).build();
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      mBackupManager = new BackupManager(mRegistry);
      mStartTimeMs = System.currentTimeMillis();
      mPort = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
      // Create masters.
      MasterUtils.createMasters(mRegistry, CoreMasterContext.newBuilder()
          .setJournalSystem(mJournalSystem)
          .setSafeModeManager(mSafeModeManager)
          .setBackupManager(mBackupManager)
          .setStartTimeMs(mStartTimeMs)
          .setPort(mPort)
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
    mRegistry.start(false);
    mLatch.await();
  }

  @Override
  public void stop() throws Exception {
    mRegistry.stop();
    mLatch.countDown();
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    return true;
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

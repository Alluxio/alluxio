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
import alluxio.master.journal.Journal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secondary Alluxio master that replays journal logs and writes checkpoints.
 */
@NotThreadSafe
public final class AlluxioSecondaryMaster implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSecondaryMaster.class);
  private final MasterRegistry mRegistry;
  private final CountDownLatch mLatch;

  /**
   * Creates a {@link AlluxioSecondaryMaster}.
   */
  AlluxioSecondaryMaster() {
    try {
      // Check that journals of each service have been formatted.
      MasterUtils.checkJournalFormatted();
      mRegistry = new MasterRegistry();
      // Create masters.
      MasterUtils.createMasters(new Journal.Factory(MasterUtils.getJournalLocation()), mRegistry);
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
  public void waitForReady() {}

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

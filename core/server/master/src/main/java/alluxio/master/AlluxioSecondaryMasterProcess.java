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
import alluxio.master.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secondary Alluxio master that replays journal logs and writes checkpoints.
 */
@NotThreadSafe
public final class AlluxioSecondaryMasterProcess implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSecondaryMasterProcess.class);
  private MasterRegistry mRegistry;

  /**
   * Creates a {@link AlluxioSecondaryMasterProcess}.
   */
  public AlluxioSecondaryMasterProcess() {
    try {
      // Check that journals of each service have been formatted.
      MasterUtils.checkJournalFormatted();
      mRegistry = new MasterRegistry();
      // Create masters.
      MasterUtils.createMasters(new Journal.Factory(MasterUtils.getJournalLocation()), mRegistry);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() throws IOException {
    try {
      connectToUFS();
      mRegistry.start(false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() throws IOException {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioSecondaryMasterProcess.class.getCanonicalName());
      System.exit(-1);
    }

    AlluxioSecondaryMasterProcess master = new AlluxioSecondaryMasterProcess();
    ProcessUtils.run(master);
  }

  /**
   * Connects to the UFS.
   *
   * @throws IOException if any I/O errors occur
   */
  private void connectToUFS() throws IOException {
    String ufsAddress = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsAddress);
    ufs.connectFromMaster(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC));
  }
}

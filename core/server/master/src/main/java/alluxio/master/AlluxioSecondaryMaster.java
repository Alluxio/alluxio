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
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.ServerUtils;
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
public final class AlluxioSecondaryMaster implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSecondaryMaster.class);
  private MasterRegistry mRegistry;

  /**
   * Creates a {@link AlluxioSecondaryMaster}.
   */
  public AlluxioSecondaryMaster() {
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
  public void start() {
    try {
      connectToUFS();
      mRegistry.start(false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Starts the secondary Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioSecondaryMaster.class.getCanonicalName());
      System.exit(-1);
    }

    AlluxioSecondaryMaster master = new AlluxioSecondaryMaster();
    ServerUtils.run(master, "Alluxio Secondary Master");
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

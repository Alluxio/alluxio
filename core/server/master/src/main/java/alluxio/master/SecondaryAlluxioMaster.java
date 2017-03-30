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

import alluxio.RuntimeConstants;
import alluxio.ServerUtils;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The secondary {@link AlluxioMaster} that only replays the journal logs and writes checkpoints.
 */
@NotThreadSafe
final class SecondaryAlluxioMaster extends DefaultAlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryAlluxioMaster.class);

  /**
   * Creates a {@link SecondaryAlluxioMaster}.
   */
  protected SecondaryAlluxioMaster() {}

  @Override
  public void start() throws Exception {
    startMasters(false);
    CommonUtils.sleepMs(LOG, 100);
  }

  @Override
  public void stop() throws Exception {
    super.stop();
  }

  /**
   * Starts the secondary Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          SecondaryAlluxioMaster.class.getCanonicalName());
      System.exit(-1);
    }

    SecondaryAlluxioMaster master = new SecondaryAlluxioMaster();
    ServerUtils.run(master, "Secondary Alluxio master");
  }
}

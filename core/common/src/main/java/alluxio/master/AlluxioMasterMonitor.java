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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.util.ShellUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Alluxio master monitor for inquiring RPC service availability.
 */
public final class AlluxioMasterMonitor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterMonitor.class);

  private Thread mMasterServingCheckThread;
  private Thread mMastersHealthCheckThread;

  /**
   *
   */
  public AlluxioMasterMonitor() {

    mMasterServingCheckThread = new Thread(() -> {
      try {
        FileSystem fs = FileSystem.Factory.get();
        fs.exists(new AlluxioURI("/"));
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }, "MasterServingCheckThread");

    mMastersHealthCheckThread = new Thread(() -> {
      MasterInquireClient client = MasterInquireClient.Factory.create();
      try {
        List<InetSocketAddress> addresses = client.getMasterRpcAddresses();
        for (InetSocketAddress address : addresses) {
          String hostname = address.getHostName();
          String cmd = "ps -ef | grep \"alluxio.master.AlluxioMaster\" | grep \"java\" | "
                  + "awk '{ print $1; }'";
          String output = ShellUtils.execCommand(
                  String.format("ssh {} {} {}", ShellUtils.COMMON_SSH_OPTS, hostname, cmd));
          if (output.isEmpty()) {
            throw new RuntimeException(
                    String.format("Master process is not running on the host {}", hostname));
          }
        }
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }, "MastersHealthCheckThread");

  }

  @Override
  public void run() {
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    try {
      executor.schedule(mMastersHealthCheckThread, 2, TimeUnit.SECONDS);
      mMasterServingCheckThread.start();
      executor.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdownNow();
      }
    }
  }

  /**
   * Starts the Alluxio master monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    AlluxioMasterMonitor monitor = new AlluxioMasterMonitor();
    try {
      monitor.run();
    } catch (Throwable t) {
      LOG.error("Exception thrown in master monitor ", t);
      System.exit(1);
    }
    System.exit(0);
  }
}

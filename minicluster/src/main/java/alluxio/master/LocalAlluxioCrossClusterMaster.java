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

import alluxio.Constants;
import alluxio.Process;
import alluxio.conf.Configuration;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Constructs an isolated master. Primary users of this class are the {@link LocalAlluxioCluster}
 * and {@link MultiMasterLocalAlluxioCluster}.
 *
 * Isolated is defined as having its own root directory, and port.
 */
@NotThreadSafe
public final class LocalAlluxioCrossClusterMaster implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioCrossClusterMaster.class);

  private final String mHostname;

  private AlluxioCrossClusterMasterProcess mMasterProcess;
  private Thread mMasterThread;

  private LocalAlluxioCrossClusterMaster() {
    mHostname = NetworkAddressUtils.getConnectHost(ServiceType.CROSS_CLUSTER_MASTER_RPC,
        Configuration.global());
  }

  /**
   * Creates a new local Alluxio master with an isolated work directory and port.
   *
   * @return an instance of Alluxio master
   */
  public static LocalAlluxioCrossClusterMaster create() {
    return new LocalAlluxioCrossClusterMaster();
  }

  /**
   * Starts the master.
   */
  public void start() {
    mMasterProcess = AlluxioCrossClusterMasterProcess.Factory.create();
    Runnable runMaster = () -> {
      try {
        LOG.info("Starting Alluxio cross cluster master {}.", mMasterProcess);
        mMasterProcess.start();
      } catch (InterruptedException e) {
        // this is expected
      } catch (Exception e) {
        // Log the exception as the RuntimeException will be caught and handled silently by JUnit
        LOG.error("Start cross cluster master error", e);
        throw new RuntimeException(
            e + " \n Start Cross Cluster Master Error \n" + e.getMessage(), e);
      }
    };
    mMasterThread = new Thread(runMaster);
    mMasterThread.setName("CrossClusterMasterThread-" + System.identityHashCode(mMasterThread));
    mMasterThread.start();
  }

  /**
   * @return true if the master is serving, false otherwise
   */
  public boolean isServing() {
    return mMasterProcess.isGrpcServing();
  }

  /**
   * Stops the master processes and cleans up client connections.
   */
  public void stop() throws Exception {
    if (mMasterThread != null) {
      mMasterProcess.stop();
      while (mMasterThread.isAlive()) {
        LOG.info("Stopping thread {}.", mMasterThread.getName());
        mMasterThread.interrupt();
        mMasterThread.join(1000);
      }
      mMasterThread = null;
    }
    System.clearProperty("alluxio.web.resources");
    System.clearProperty("alluxio.master.min.worker.threads");
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor("Cross cluster master serving", this::isServing,
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
    return true;
  }

  /**
   * @return the externally resolvable address of the master
   */
  public InetSocketAddress getAddress() {
    return mMasterProcess.getRpcAddress();
  }

  /**
   * @return the internal {@link MasterProcess}
   */
  public AlluxioCrossClusterMasterProcess getMasterProcess() {
    return mMasterProcess;
  }

  /**
   * Gets the actual port that the RPC service is listening on.
   *
   * @return the RPC local port
   */
  public int getRpcLocalPort() {
    return mMasterProcess.getRpcAddress().getPort();
  }

  /**
   * @return the URI of the master
   */
  public String getUri() {
    return Constants.HEADER + mHostname + ":" + getRpcLocalPort();
  }
}

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
import alluxio.Constants;
import alluxio.HealthCheckClient;
import alluxio.client.file.FileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ShellUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * MasterHealthCheckClient check whether Alluxio master is serving RPC requests and
 * if the AlluxioMaster process is running in all master hosts.
 */
public class MasterHealthCheckClient implements HealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(MasterHealthCheckClient.class);
  private Runnable mMasterServingCheckRunnable;
  private Runnable mProcessCheckRunnable;

  /**
   * Creates a master health check client.
   */
  public MasterHealthCheckClient() {
    mMasterServingCheckRunnable = () -> {
      try {
        LOG.debug("Checking master is serving requests");
        FileSystem fs = FileSystem.Factory.get();
        fs.exists(new AlluxioURI("/"));
        LOG.debug("Master is serving requests");
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };

    mProcessCheckRunnable = () -> {
      MasterInquireClient client = MasterInquireClient.Factory.create();
      try {
        while (true) {
          List<InetSocketAddress> addresses = client.getMasterRpcAddresses();
          for (InetSocketAddress address : addresses) {
            String host = address.getHostName();
            LOG.debug("Master health check on node {}", host);
            String cmd = String.format("ssh %s %s %s", ShellUtils.COMMON_SSH_OPTS, host,
                    "ps -ef | "
                            + "grep -v \"alluxio.master.AlluxioMasterMonitor\" | "
                            + "grep \"alluxio.master.AlluxioMaster\" | "
                            + "grep \"java\" | "
                            + "awk '{ print $2; }'");
            LOG.debug("Executing: {}", cmd);
            String output = ShellUtils.execCommand("bash", "-c", cmd);
            if (output.isEmpty()) {
              throw new IllegalStateException(
                      String.format("Master process is not running on the host %s", host));
            }
            LOG.debug("Master running on node {} with pid={}", host, output);
          }
          CommonUtils.sleepMs(Constants.SECOND_MS);
        }
      } catch (Throwable e) {
        LOG.error("Exception thrown in the master process check {}", e);
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public boolean isServing() {
    ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
    try {
      Future<?> masterServingCheck = mExecutorService.submit(mMasterServingCheckRunnable);
      Future<?> processCheck = mExecutorService.submit(mProcessCheckRunnable);
      while (!masterServingCheck.isDone()) {
        if (processCheck.isDone()) {
          throw new IllegalStateException("One or more master processes are not running");
        }
        CommonUtils.sleepMs(Constants.SECOND_MS);
      }
      CommonUtils.sleepMs(Constants.SECOND_MS);
      LOG.debug("Checking the master processes one more time...");
      return !processCheck.isDone();
    } catch (Exception e) {
      LOG.error("Exception thrown in master health check client {}", e);
    } finally {
      mExecutorService.shutdown();
    }
    return false;
  }
}

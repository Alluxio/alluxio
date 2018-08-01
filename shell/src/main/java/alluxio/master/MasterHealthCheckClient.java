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
  private String mAlluxioMasterName;
  private boolean mProcessCheck;
  private ExecutorService mExecutorService;

  /**
   * Builder for a {@link MasterHealthCheckClient}.
   */
  public static class Builder {
    private boolean mProcessCheck;
    private String mAlluxioMasterName;

    /**
     * Constructs the builder with default values.
     */
    public Builder() {
      mProcessCheck = true;
      mAlluxioMasterName = "alluxio.master.AlluxioMaster";
    }

    /**
     * @param processCheck whether to check the AlluxioMaster process is alive
     * @return an instance of the builder
     */
    public Builder withProcessCheck(boolean processCheck) {
      mProcessCheck = processCheck;
      return this;
    }

    /**
     * @param alluxioMasterName the Alluxio master process name
     * @return an instance of the builder
     */
    public Builder withAlluxioMasterName(String alluxioMasterName) {
      mAlluxioMasterName = alluxioMasterName;
      return this;
    }

    /**
     * @return a {@link MasterHealthCheckClient} for the current builder values
     */
    public HealthCheckClient build() {
      return new MasterHealthCheckClient(mAlluxioMasterName, mProcessCheck);
    }
  }

  /**
   * Runnable for checking if the AlluxioMaster is serving RPCs.
   */
  public final class MasterServingCheckRunnable implements Runnable {

    @Override
    public void run() {
      try {
        LOG.debug("Checking master is serving requests");
        FileSystem fs = FileSystem.Factory.get();
        fs.exists(new AlluxioURI("/"));
        LOG.debug("Master is serving requests");
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Runnable for checking if the AlluxioMaster process are running in all the masters hosts.
   * This include both primary and stand-by masters.
   */
  public final class ProcessCheckRunnable implements Runnable {
    private String mAlluxioMasterName;

    /**
     * Creates a new instance of ProcessCheckRunnable.
     *
     * @param alluxioMasterName the Alluxio master process name
     */
    public ProcessCheckRunnable(String alluxioMasterName) {
      mAlluxioMasterName = alluxioMasterName;
    }

    @Override
    public void run() {
      MasterInquireClient client = MasterInquireClient.Factory.create();
      try {
        while (true) {
          List<InetSocketAddress> addresses = client.getMasterRpcAddresses();
          for (InetSocketAddress address : addresses) {
            String host = address.getHostName();
            int port = address.getPort();
            LOG.debug("Master health check on node {}", host);
            String cmd = String.format("ssh %s %s %s", ShellUtils.COMMON_SSH_OPTS, host,
                "ps -ef | grep \"" + mAlluxioMasterName + "$\" | "
                + "grep \"java\" | "
                + "awk '{ print $2; }'");
            LOG.debug("Executing: {}", cmd);
            String output = ShellUtils.execCommand("bash", "-c", cmd);
            if (output.isEmpty()) {
              throw new IllegalStateException(
                  String.format("Master process is not running on the host %s", host));
            } else if (output.contains("Connection refused")) {
              throw new IllegalStateException(
                  String.format("Connection refused while connecting to the host %s on port %d",
                      host, port));
            }
            LOG.debug("Master running on node {} with pid={}", host, output);
          }
          CommonUtils.sleepMs(Constants.SECOND_MS);
        }
      } catch (Throwable e) {
        LOG.error("Exception thrown in the master process check {}", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Creates a new instance of MasterHealthCheckClient.
   *
   * @param alluxioMasterName the Alluxio master process name
   * @param processCheck whether to check the AlluxioMaster process is alive
   */
  public MasterHealthCheckClient(String alluxioMasterName, boolean processCheck) {
    mAlluxioMasterName = alluxioMasterName;
    mProcessCheck = processCheck;
    mExecutorService = Executors.newFixedThreadPool(2);
  }

  @Override
  public boolean isServing() {
    try {
      Future<?> masterServingCheckFuture = mExecutorService.submit(
              new MasterServingCheckRunnable());
      if (mProcessCheck) {
        Future<?> processCheckFuture = mExecutorService.submit(
                new ProcessCheckRunnable(mAlluxioMasterName));
        CommonUtils.sleepMs(Constants.SECOND_MS);
        while (!masterServingCheckFuture.isDone()) {
          if (processCheckFuture.isDone()) {
            throw new IllegalStateException("One or more master processes are not running");
          }
          CommonUtils.sleepMs(Constants.SECOND_MS);
        }
        CommonUtils.sleepMs(Constants.SECOND_MS);
        LOG.debug("Checking the master processes one more time...");
        return !processCheckFuture.isDone();
      } else {
        masterServingCheckFuture.get();
        return true;
      }
    } catch (Exception e) {
      LOG.error("Exception thrown in master health check client {}", e);
    } finally {
      mExecutorService.shutdown();
    }
    return false;
  }
}

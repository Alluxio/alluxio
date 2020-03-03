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
import alluxio.HealthCheckClient;
import alluxio.common.RpcPortHealthCheckClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ServiceType;
import alluxio.retry.RetryPolicy;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * MasterHealthCheckClient check whether Alluxio master is serving RPC requests and
 * if the AlluxioMaster process is running in all master hosts.
 */
public class MasterHealthCheckClient implements HealthCheckClient {
  private static final Logger LOG = LoggerFactory.getLogger(MasterHealthCheckClient.class);
  private MasterType mAlluxioMasterType;
  private boolean mProcessCheck;
  private ExecutorService mExecutorService;
  private Supplier<RetryPolicy> mPolicySupplier;
  private AlluxioConfiguration mConf;

  /**
   * An enum mapping master types to fully qualified class names.
   */
  public enum MasterType {
    MASTER("alluxio.master.AlluxioMaster"),
    JOB_MASTER("alluxio.master.AlluxioJobMaster")
    ;

    private String mClassName;

    /**
     * Creates a new instance of {@link MasterType}.
     *
     * @param className the fully qualified class name of the master
     */
    MasterType(String className) {
      mClassName = className;
    }

    /**
     * @return the fully qualified classname associated with the master type
     */
    public String getClassName() {
      return mClassName;
    }
  }

  /**
   * Builder for a {@link MasterHealthCheckClient}.
   */
  public static class Builder {

    private boolean mProcessCheck;
    private MasterType mAlluxioMasterType;
    private AlluxioConfiguration mConf;
    private Supplier<RetryPolicy> mPolicySupplier;

    /**
     * Constructs the builder with default values.
     *
     * @param alluxioConf Alluxio configuration
     */
    public Builder(AlluxioConfiguration alluxioConf) {
      mProcessCheck = true;
      mAlluxioMasterType = MasterType.MASTER;
      mConf = alluxioConf;
      mPolicySupplier = AlluxioMasterMonitor.TWO_MIN_EXP_BACKOFF;
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
     *
     * @param alluxioConf Alluxio configuration
     * @return a builder which utlizes the given alluxio configuration
     */
    public Builder withConfiguration(AlluxioConfiguration alluxioConf) {
      mConf = alluxioConf;
      return this;
    }

    /**
     * @param masterType the Alluxio master type
     * @return an instance of the builder
     */
    public Builder withAlluxioMasterType(MasterType masterType) {
      mAlluxioMasterType = masterType;
      return this;
    }

    /**
     * @param policySupplier the retry policy supplier to use for the health check
     * @return a builder utilizing the given retry policy supplier
     */
    public Builder withRetryPolicy(Supplier<RetryPolicy> policySupplier) {
      mPolicySupplier = policySupplier;
      return this;
    }

    /**
     * @return a {@link MasterHealthCheckClient} for the current builder values
     */
    public HealthCheckClient build() {
      return new MasterHealthCheckClient(mAlluxioMasterType, mProcessCheck, mPolicySupplier, mConf);
    }
  }

  /**
   * Runnable for checking if the AlluxioMaster is serving RPCs.
   */
  public final class MasterServingHealthCheck extends RpcPortHealthCheckClient implements Runnable {
    private AtomicBoolean mIsServing = new AtomicBoolean(false);

    /**
     * Creates a new instance of {@link MasterServingHealthCheck} to check for an open RPC port
     * for the given service.
     *
     * @param nodeAddress the address of the node to connect to
     * @param serviceType the gRPC service type
     * @param retryPolicySupplier a retry policy supplier
     * @param alluxioConf Alluxio configuration
     */
    public MasterServingHealthCheck(InetSocketAddress nodeAddress, ServiceType serviceType,
        Supplier<RetryPolicy> retryPolicySupplier, AlluxioConfiguration alluxioConf) {
      super(nodeAddress, serviceType, retryPolicySupplier, alluxioConf);
    }

    @Override
    public void run() {
      if (super.isServing()) {
        mIsServing.set(true);
      }
    }

    /**
     * @return whether or we were able to ping the service
     */
    public boolean serving() {
      return mIsServing.get();
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
      UserState userState = UserState.Factory.create(mConf);
      MasterInquireClient client = MasterInquireClient.Factory.create(mConf, userState);
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
        LOG.error("Exception thrown in the master process check", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Creates a new instance of MasterHealthCheckClient.
   *
   * @param alluxioMasterType the Alluxio master process type
   * @param processCheck whether to check the AlluxioMaster process is alive
   * @param retryPolicySupplier the policy supplier to utilize for the rpc check
   * @param alluxioConf Alluxio configuration
   */
  public MasterHealthCheckClient(MasterType alluxioMasterType, boolean processCheck,
      Supplier<RetryPolicy> retryPolicySupplier, AlluxioConfiguration alluxioConf) {
    mAlluxioMasterType = alluxioMasterType;
    mProcessCheck = processCheck;
    mExecutorService = Executors.newFixedThreadPool(2);
    mPolicySupplier = retryPolicySupplier;
    mConf = alluxioConf;
  }

  @Override
  public boolean isServing() {
    try {
      ServiceType rpcService;
      InetSocketAddress connectAddr;
      switch (mAlluxioMasterType) {
        case MASTER:
          rpcService = ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
          connectAddr = NetworkAddressUtils
            .getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC, mConf);
          break;
        case JOB_MASTER:
          rpcService = ServiceType.JOB_MASTER_CLIENT_SERVICE;
          connectAddr = NetworkAddressUtils
              .getConnectAddress(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC, mConf);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Master type %s is invalid", mAlluxioMasterType));
      }
      MasterServingHealthCheck masterRpcCheck =
          new MasterServingHealthCheck(connectAddr, rpcService,
              mPolicySupplier,
              mConf);
      Future<?> masterServingFuture = mExecutorService.submit(masterRpcCheck);
      if (mProcessCheck) {
        Future<?> processCheckFuture = mExecutorService.submit(
                new ProcessCheckRunnable(mAlluxioMasterType.getClassName()));
        CommonUtils.sleepMs(Constants.SECOND_MS);
        // If in HA mode, can't check the RPC service, because the service may not have started
        if (!ConfigurationUtils.isHaMode(mConf)) {
          while (!masterServingFuture.isDone()) {
            if (processCheckFuture.isDone()) {
              throw new IllegalStateException("One or more master processes are not running");
            }
            CommonUtils.sleepMs(Constants.SECOND_MS);
          }
        }
        // Check after a 7 second period if the process is is still alive
        CommonUtils.sleepMs(7L * Constants.SECOND_MS);
        LOG.debug("Checking the master processes one more time...");
        return !processCheckFuture.isDone();
      } else {
        masterServingFuture.get();
        return masterRpcCheck.serving();
      }
    } catch (Exception e) {
      LOG.error("Exception thrown in master health check client", e);
    } finally {
      mExecutorService.shutdown();
    }
    return false;
  }
}

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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.JournalDomain;
import alluxio.master.cross.cluster.CrossClusterMaster;
import alluxio.master.cross.cluster.DefaultCrossClusterMaster;
import alluxio.master.journal.AlwaysPrimaryPrimarySelector;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.CrossClusterMasterWebServer;
import alluxio.web.WebServer;

import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is responsible for initializing Cross Cluster master.
 */
@NotThreadSafe
public class AlluxioCrossClusterMasterProcess extends AlluxioBaseMasterProcess {

  protected CrossClusterMaster mCrossClusterMaster;

  AlluxioCrossClusterMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector) {
    super("cross cluster", JournalDomain.CROSS_CLUSTER_MASTER, journalSystem, leaderSelector,
        ServiceType.CROSS_CLUSTER_MASTER_WEB, ServiceType.CROSS_CLUSTER_MASTER_RPC,
        PropertyKey.CROSS_CLUSTER_MASTER_HOSTNAME);
      // Create master.
    mCrossClusterMaster = new DefaultCrossClusterMaster(
        new MasterContext<>(mJournalSystem, null, new NoopUfsManager()));
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    if (clazz == CrossClusterMaster.class) {
      return (T) mCrossClusterMaster;
    } else {
      throw new RuntimeException(String.format("Could not find the master: %s", clazz));
    }
  }

  @Override
  AbstractMaster getAbstractMaster() {
    return (AbstractMaster) mCrossClusterMaster;
  }

  @Override
  WebServer createWebServer() {
    return new CrossClusterMasterWebServer(ServiceType.CROSS_CLUSTER_MASTER_WEB.getServiceName(),
        mWebBindAddress, this);
  }

  @Override
  GrpcServerBuilder createBaseRPCServer() {
    return GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            Configuration.global())
        .flowControlWindow(
            (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) Configuration
            .getBytes(PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
  }

  /**
   * Factory for creating {@link AlluxioCrossClusterMasterProcess}.
   */
  @ThreadSafe
  static final class Factory {
    /**
     * Factory to create an instance of a cross cluster master process.
     * Currently, this process only supports a no-op journal system, and
     * a single master who is always a primary.
     * @return a new instance of {@link AlluxioCrossClusterMasterProcess}
     */
    public static AlluxioCrossClusterMasterProcess create() {
      JournalSystem journalSystem = new NoopJournalSystem();
      return new AlluxioCrossClusterMasterProcess(journalSystem,
          new AlwaysPrimaryPrimarySelector());
    }

    private Factory() {} // prevent instantiation
  }
}

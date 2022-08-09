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

package alluxio.master.cross.cluster;

import alluxio.Constants;
import alluxio.clock.SystemClock;
import alluxio.grpc.GrpcService;
import alluxio.grpc.MountList;
import alluxio.grpc.ServiceType;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Master for cross cluster configuration services.
 */
public class DefaultCrossClusterMaster extends CoreMaster implements CrossClusterMaster {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultCrossClusterMaster.class);

  private final CrossClusterState mCrossClusterState = new CrossClusterState();

  /** Core master context. */
  private final CoreMasterContext mCoreMasterContext;

  DefaultCrossClusterMaster(CoreMasterContext masterContext) {
    super(masterContext, new SystemClock(), ExecutorServiceFactories.cachedThreadPool(
        Constants.CROSS_CLUSTER_MASTER_NAME));
    mCoreMasterContext = masterContext;
  }

  /**
   * @return the cross cluster state object
   */
  @VisibleForTesting
  public CrossClusterState getCrossClusterState() {
    return mCrossClusterState;
  }

  @Override
  public void subscribeMounts(String clusterId, StreamObserver<MountList> stream) {
    mCrossClusterState.setStream(clusterId, stream);
  }

  @Override
  public void setMountList(MountList mountList) {
    mCrossClusterState.setMountList(mountList);
  }

  @Override
  public String getName() {
    return Constants.CROSS_CLUSTER_MASTER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.CROSS_CLUSTER_MASTER_CLIENT_SERVICE,
        new GrpcService(new CrossClusterMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return null;
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    return false;
  }

  @Override
  public void resetState() {
    try {
      mCrossClusterState.close();
    } catch (IOException e) {
      LOG.warn("Error while resetting state", e);
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.CROSS_CLUSTER_MASTER;
  }
}

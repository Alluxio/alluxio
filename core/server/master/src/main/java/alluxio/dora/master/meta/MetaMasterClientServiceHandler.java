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

package alluxio.dora.master.meta;

import alluxio.dora.RpcUtils;
import alluxio.dora.RuntimeConstants;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.grpc.BackupPRequest;
import alluxio.dora.grpc.BackupPStatus;
import alluxio.dora.grpc.BackupStatusPRequest;
import alluxio.dora.grpc.CheckpointPOptions;
import alluxio.dora.grpc.CheckpointPResponse;
import alluxio.dora.grpc.GetConfigReportPOptions;
import alluxio.dora.grpc.GetConfigReportPResponse;
import alluxio.dora.grpc.GetMasterInfoPOptions;
import alluxio.dora.grpc.GetMasterInfoPResponse;
import alluxio.dora.grpc.MasterInfo;
import alluxio.dora.grpc.MasterInfoField;
import alluxio.dora.grpc.MetaMasterClientServiceGrpc;
import alluxio.dora.master.StateLockOptions;
import alluxio.dora.master.journal.raft.RaftJournalSystem;
import alluxio.dora.wire.Address;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is a gRPC handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler
    extends MetaMasterClientServiceGrpc.MetaMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterClientServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public void backup(BackupPRequest request, StreamObserver<BackupPStatus> responseObserver) {
    RpcUtils.call(LOG,
        () -> mMetaMaster.backup(request, StateLockOptions.defaultsForShellBackup()).toProto(),
        "backup", "request=%s", responseObserver, request);
  }

  @Override
  public void getBackupStatus(BackupStatusPRequest request,
      StreamObserver<BackupPStatus> responseObserver) {
    RpcUtils.call(LOG, () -> mMetaMaster.getBackupStatus(request).toProto(),
        "getBackupStatus", "request=%s", responseObserver, request);
  }

  @Override
  public void getConfigReport(GetConfigReportPOptions options,
      StreamObserver<GetConfigReportPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> GetConfigReportPResponse.newBuilder()
            .setReport(mMetaMaster.getConfigCheckReport().toProto()).build(),
        "getConfigReport", "options=%s", responseObserver, options);
  }

  @Override
  public void getMasterInfo(GetMasterInfoPOptions options,
      StreamObserver<GetMasterInfoPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      MasterInfo.Builder masterInfo = MasterInfo.newBuilder();
      for (MasterInfoField field : options.getFilterCount() > 0 ? options.getFilterList()
          : Arrays.asList(MasterInfoField.values())) {
        switch (field) {
          case CLUSTER_ID:
            masterInfo.setClusterId(mMetaMaster.getClusterID());
            break;
          case LEADER_MASTER_ADDRESS:
            masterInfo.setLeaderMasterAddress(mMetaMaster.getRpcAddress().toString());
            break;
          case MASTER_ADDRESSES:
            masterInfo.addAllMasterAddresses(mMetaMaster.getMasterAddresses().stream()
                .map(Address::toProto).collect(Collectors.toList()));
            break;
          case RPC_PORT:
            masterInfo.setRpcPort(mMetaMaster.getRpcAddress().getPort());
            break;
          case SAFE_MODE:
            masterInfo.setSafeMode(mMetaMaster.isInSafeMode());
            break;
          case START_TIME_MS:
            masterInfo.setStartTimeMs(mMetaMaster.getStartTimeMs());
            break;
          case UP_TIME_MS:
            masterInfo.setUpTimeMs(mMetaMaster.getUptimeMs());
            break;
          case VERSION:
            masterInfo.setVersion(RuntimeConstants.VERSION);
            break;
          case WEB_PORT:
            masterInfo.setWebPort(mMetaMaster.getWebPort());
            break;
          case WORKER_ADDRESSES:
            masterInfo.addAllWorkerAddresses(mMetaMaster.getWorkerAddresses().stream()
                .map(Address::toProto).collect(Collectors.toList()));
            break;
          case ZOOKEEPER_ADDRESSES:
            if (Configuration.isSet(PropertyKey.ZOOKEEPER_ADDRESS)) {
              masterInfo.addAllZookeeperAddresses(
                  Arrays.asList(Configuration.getString(PropertyKey.ZOOKEEPER_ADDRESS)
                      .split(",")));
            }
            break;
          case RAFT_ADDRESSES:
            if (mMetaMaster.getMasterContext().getJournalSystem() instanceof RaftJournalSystem) {
              List<String> raftAddresses =
                  ((RaftJournalSystem) mMetaMaster.getMasterContext().getJournalSystem())
                      .getQuorumServerInfoList().stream().map(info -> String.format("%s:%d",
                          info.getServerAddress().getHost(),
                          info.getServerAddress().getRpcPort()))
                      .collect(Collectors.toList());
              masterInfo.addAllRaftAddress(raftAddresses);
            }
            break;
          case RAFT_JOURNAL:
            masterInfo.setRaftJournal(mMetaMaster.getMasterContext().getJournalSystem()
                instanceof RaftJournalSystem);
            break;
          default:
            LOG.warn("Unrecognized meta master info field: " + field);
        }
      }
      return GetMasterInfoPResponse.newBuilder().setMasterInfo(masterInfo).build();
    }, "getMasterInfo", "options=%s", responseObserver, options);
  }

  @Override
  public void checkpoint(CheckpointPOptions options,
      StreamObserver<CheckpointPResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> CheckpointPResponse.newBuilder().setMasterHostname(mMetaMaster.checkpoint()).build(),
        "checkpoint", "options=%s", responseObserver, options);
  }
}

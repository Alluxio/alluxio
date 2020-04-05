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

package alluxio.master.meta;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetMasterIdPRequest;
import alluxio.grpc.MasterHeartbeatPRequest;
import alluxio.grpc.MetaCommand;
import alluxio.grpc.MetaMasterMasterServiceGrpc;
import alluxio.grpc.RegisterMasterPOptions;
import alluxio.grpc.RegisterMasterPRequest;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the primary meta master,
 * used by Alluxio standby masters.
 */
@ThreadSafe
public final class RetryHandlingMetaMasterMasterClient extends AbstractMasterClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(RetryHandlingMetaMasterMasterClient.class);
  private MetaMasterMasterServiceGrpc.MetaMasterMasterServiceBlockingStub mClient = null;

  /**
   * Creates a instance of {@link RetryHandlingMetaMasterMasterClient}.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetaMasterMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.META_MASTER_MASTER_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_MASTER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_MASTER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = MetaMasterMasterServiceGrpc.newBlockingStub(mChannel);
  }

  /**
   * Returns a master id for a master address.
   *
   * @param address the address to get a master id for
   * @return a master id
   */
  public long getId(final Address address) throws IOException {
    return retryRPC(() -> mClient
        .getMasterId(GetMasterIdPRequest.newBuilder().setMasterAddress(address.toProto()).build())
        .getMasterId(), LOG, "GetId", "address=%s", address);
  }

  /**
   * Sends a heartbeat to the leader master. Standby masters periodically execute this method
   * so that the leader master knows they are still running.
   *
   * @param masterId the master id
   * @return whether this master should re-register
   */
  public MetaCommand heartbeat(final long masterId) throws IOException {
    return retryRPC(() -> mClient
        .masterHeartbeat(MasterHeartbeatPRequest.newBuilder().setMasterId(masterId).build())
        .getCommand(), LOG, "Heartbeat", "masterId=%d", masterId);
  }

  /**
   * Registers with the leader master.
   *
   * @param masterId the master id of the standby master registering
   * @param configList the configuration of this master
   */
  public void register(final long masterId, final List<ConfigProperty> configList)
      throws IOException {
    retryRPC(() -> {
      mClient.registerMaster(RegisterMasterPRequest.newBuilder().setMasterId(masterId)
          .setOptions(RegisterMasterPOptions.newBuilder().addAllConfigs(configList).build())
          .build());
      return null;
    }, LOG, "Register", "masterId=%d,configList=%s", masterId, configList);
  }
}

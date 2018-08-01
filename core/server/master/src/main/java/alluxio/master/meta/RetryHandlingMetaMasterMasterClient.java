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
import alluxio.master.MasterClientConfig;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.GetMasterIdTOptions;
import alluxio.thrift.MasterHeartbeatTOptions;
import alluxio.thrift.MetaCommand;
import alluxio.thrift.MetaMasterMasterService;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.wire.Address;
import alluxio.wire.ConfigProperty;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the primary meta master,
 * used by Alluxio standby masters.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingMetaMasterMasterClient extends AbstractMasterClient {
  private MetaMasterMasterService.Client mClient = null;

  /**
   * Creates a instance of {@link RetryHandlingMetaMasterMasterClient}.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetaMasterMasterClient(MasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
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
    mClient = new MetaMasterMasterService.Client(mProtocol);
  }

  /**
   * Returns a master id for a master address.
   *
   * @param address the address to get a master id for
   * @return a master id
   */
  public synchronized long getId(final Address address) throws IOException {
    return retryRPC(() -> mClient
        .getMasterId(address.toThrift(), new GetMasterIdTOptions()).getMasterId());
  }

  /**
   * Sends a heartbeat to the leader master. Standby masters periodically execute this method
   * so that the leader master knows they are still running.
   *
   * @param masterId the master id
   * @return whether this master should re-register
   */
  public synchronized MetaCommand heartbeat(final long masterId) throws IOException {
    return retryRPC(() ->
        mClient.masterHeartbeat(masterId, new MasterHeartbeatTOptions()).getCommand());
  }

  /**
   * Registers with the leader master.
   *
   * @param masterId the master id of the standby master registering
   * @param configList the configuration of this master
   */
  public synchronized void register(final long masterId,
      final List<ConfigProperty> configList) throws IOException {
    retryRPC(() -> {
      mClient.registerMaster(masterId, new RegisterMasterTOptions(configList
          .stream().map(ConfigProperty::toThrift).collect(Collectors.toList())));
      return null;
    });
  }
}

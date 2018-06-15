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

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetMasterIdTOptions;
import alluxio.thrift.GetMasterIdTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.MasterHeartbeatTOptions;
import alluxio.thrift.MasterHeartbeatTResponse;
import alluxio.thrift.MasterNetAddress;
import alluxio.thrift.MetaMasterMasterService;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.thrift.RegisterMasterTResponse;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for meta master RPCs invoked by an Alluxio standby master.
 */
@NotThreadSafe
public final class MetaMasterMasterServiceHandler implements MetaMasterMasterService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterMasterServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * Creates a new instance of {@link MetaMasterMasterServiceHandler}.
   *
   * @param metaMaster the Alluxio meta master
   */
  MetaMasterMasterServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.META_MASTER_MASTER_SERVICE_VERSION);
  }

  @Override
  public GetMasterIdTResponse getMasterId(final MasterNetAddress address,
      GetMasterIdTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<GetMasterIdTResponse>() {
      @Override
      public GetMasterIdTResponse call() throws AlluxioException {
        return new GetMasterIdTResponse(mMetaMaster
            .getMasterId(Address.fromThrift(address)));
      }

      @Override
      public String toString() {
        return String
            .format("getMasterId: address=%s, options=%s", address, options);
      }
    });
  }

  @Override
  public MasterHeartbeatTResponse masterHeartbeat(final long masterId,
       final MasterHeartbeatTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<MasterHeartbeatTResponse>() {
      @Override
      public MasterHeartbeatTResponse call() throws AlluxioException {
        return new MasterHeartbeatTResponse(mMetaMaster.masterHeartbeat(masterId));
      }

      @Override
      public String toString() {
        return String.format("masterHeartbeat: masterId=%s, options=%s", masterId, options);
      }
    });
  }

  @Override
  public RegisterMasterTResponse registerMaster(final long masterId,
      RegisterMasterTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallableThrowsIOException<RegisterMasterTResponse>() {
      @Override
      public RegisterMasterTResponse call() throws AlluxioException, AlluxioStatusException {
        mMetaMaster.masterRegister(masterId, options);
        return new RegisterMasterTResponse();
      }

      @Override
      public String toString() {
        return String.format("registerMaster: masterId=%s, options=%s", masterId, options);
      }
    });
  }
}

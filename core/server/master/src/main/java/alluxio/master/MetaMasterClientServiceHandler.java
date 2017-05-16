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
import alluxio.RpcUtils;
import alluxio.exception.AlluxioException;
import alluxio.thrift.GetMasterInfoTOptions;
import alluxio.thrift.GetMasterInfoTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.MasterInfo;
import alluxio.thrift.MasterInfoField;
import alluxio.thrift.MetaMasterClientService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This class is a Thrift handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler implements MetaMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MasterProcess mMasterProcess;

  /**
   * @param masterProcess the Alluxio master process
   */
  MetaMasterClientServiceHandler(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.META_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetMasterInfoTResponse getMasterInfo(final GetMasterInfoTOptions options)
      throws TException {
    return RpcUtils.call(LOG, new RpcUtils.RpcCallable<GetMasterInfoTResponse>() {
      @Override
      public GetMasterInfoTResponse call() throws AlluxioException {
        MasterInfo info = new alluxio.thrift.MasterInfo();
        for (MasterInfoField field : options.getFilter() != null ? options.getFilter()
            : Arrays.asList(MasterInfoField.values())) {
          switch (field) {
            case WEB_PORT:
              info.setWebPort(mMasterProcess.getWebAddress().getPort());
              break;
            default:
              LOG.warn("Unrecognized master info field: " + field);
          }
        }
        return new GetMasterInfoTResponse(info);
      }
    });
  }
}

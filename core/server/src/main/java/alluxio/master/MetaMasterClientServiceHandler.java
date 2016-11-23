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
import alluxio.thrift.MasterInfo;
import alluxio.thrift.MetaMasterClientService;

import org.apache.thrift.TException;

import java.util.List;

/**
 * This class is a Thrift handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler implements MetaMasterClientService.Iface {
  private final AlluxioMasterService mAlluxioMaster;

  /**
   * @param alluxioMaster the Alluxio master
   */
  public MetaMasterClientServiceHandler(AlluxioMasterService alluxioMaster) {
    mAlluxioMaster = alluxioMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.META_MASTER_CLIENT_SERVICE_VERSION;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MasterInfo getMasterInfo(List<String> fieldNames) throws TException {
    return null;
  }
}

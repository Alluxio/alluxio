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
import alluxio.thrift.MasterInfoField;
import alluxio.thrift.MetaMasterClientService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is a Thrift handler for meta master RPCs.
 */
public final class MetaMasterClientServiceHandler implements MetaMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterClientServiceHandler.class);

  private final MasterProcess mMasterProcess;

  /**
   * @param masterProcess the Alluxio master process
   */
  public MetaMasterClientServiceHandler(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  @Override
  public long getServiceVersion() {
    return Constants.META_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public MasterInfo getInfo(Set<MasterInfoField> fields) throws TException {
    if (fields == null) {
      fields = new HashSet<>(Arrays.asList(MasterInfoField.values()));
    }
    MasterInfo info = new alluxio.thrift.MasterInfo();
    for (MasterInfoField field : fields) {
      switch (field) {
        case WEB_PORT:
          info.setWebPort(mMasterProcess.getWebAddress().getPort());
          break;
        default:
          LOG.warn("Unrecognized master info field: " + field);
      }
    }
    return info;
  }
}

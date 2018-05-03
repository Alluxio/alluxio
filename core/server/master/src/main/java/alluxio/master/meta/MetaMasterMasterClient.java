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
import alluxio.thrift.MetaMasterMasterService;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * A wrapper for the thrift client to interact with the primary meta master,
 * used by Alluxio standby masters.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class MetaMasterMasterClient extends AbstractMasterClient {
  private MetaMasterMasterService.Client mClient = null;

  /**
   * Creates a instance of {@link MetaMasterMasterClient}.
   *
   * @param conf master client configuration
   */
  public MetaMasterMasterClient(MasterClientConfig conf) {
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
   * Returns a master id for a master hostname.
   *
   * @param hostname the hostname to get a master id for
   * @return a master id
   */
  public synchronized long getId(final String hostname) throws IOException {
    return retryRPC(() -> mClient.getMasterId(hostname, new GetMasterIdTOptions())
        .getMasterId());
  }
}

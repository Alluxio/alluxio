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

package alluxio;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.retry.RetryPolicy;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  /** Client for determining the master RPC address. */
  private final MasterInquireClient mMasterInquireClient;

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param alluxioConf Alluxio's configuration
   */
  public AbstractMasterClient(MasterClientConfig clientConf, AlluxioConfiguration alluxioConf) {
    super(clientConf.getSubject(), alluxioConf, null);
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param address address to connect to
   * @param retryPolicySupplier retry policy to use
   * @param alluxioConf Alluxio's configuration
   */
  public AbstractMasterClient(MasterClientConfig clientConf, AlluxioConfiguration alluxioConf,
      InetSocketAddress address, Supplier<RetryPolicy> retryPolicySupplier) {
    super(clientConf.getSubject(), alluxioConf, address, retryPolicySupplier);
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  @Override
  public synchronized InetSocketAddress getAddress() throws UnavailableException {
    return mMasterInquireClient.getPrimaryRpcAddress();
  }
}

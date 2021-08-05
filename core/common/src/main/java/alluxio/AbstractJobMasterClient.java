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

import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.retry.RetryPolicy;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for job master clients.
 */
@ThreadSafe
public abstract class AbstractJobMasterClient extends AbstractMasterClient{
  /** Address to load configuration, which may differ from {@code mAddress}. */
  protected InetSocketAddress mConfAddress;

  /** Client for determining configuration RPC address,
   * which may be different from target address. */
  private final MasterInquireClient mConfMasterInquireClient;

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   */
  public AbstractJobMasterClient(MasterClientContext clientConf) {
    super(clientConf);
    mConfMasterInquireClient = clientConf.getConfMasterInquireClient();
  }

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param address address to connect to
   * @param retryPolicySupplier retry policy to use
   */
  public AbstractJobMasterClient(MasterClientContext clientConf, InetSocketAddress address,
                              Supplier<RetryPolicy> retryPolicySupplier) {
    super(clientConf, address, retryPolicySupplier);
    mConfMasterInquireClient = clientConf.getConfMasterInquireClient();
  }

  @Override
  public synchronized InetSocketAddress getConfAddress() throws UnavailableException {
    return mConfMasterInquireClient.getPrimaryRpcAddress();
  }
}

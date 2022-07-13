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
import alluxio.grpc.GrpcServerAddress;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.retry.RetryPolicy;

import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

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
   */
  public AbstractMasterClient(MasterClientContext clientConf) {
    super(clientConf);
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param retryPolicySupplier retry policy to use
   */
  public AbstractMasterClient(MasterClientContext clientConf,
                              Supplier<RetryPolicy> retryPolicySupplier) {
    super(clientConf, retryPolicySupplier);
    mMasterInquireClient = clientConf.getMasterInquireClient();
  }

  @Override
  public synchronized GrpcServerAddress queryGrpcServerAddress() throws UnavailableException {
    return GrpcServerAddress.create(mMasterInquireClient.getPrimaryRpcAddress());
  }
}

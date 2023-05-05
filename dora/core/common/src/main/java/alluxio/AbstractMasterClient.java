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
import alluxio.master.selectionpolicy.MasterSelectionPolicy;
import alluxio.retry.RetryPolicy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  /** Client for determining the master RPC address. */
  private final MasterInquireClient mMasterInquireClient;

  private final MasterSelectionPolicy mMasterSelectionPolicy;

  /**
   * Creates a new master client base and default the selection policy to primary master.
   *
   * @param clientConf master client configuration
   */
  public AbstractMasterClient(MasterClientContext clientConf) {
    this(clientConf, MasterSelectionPolicy.Factory.primaryMaster());
  }

  /**
   * Creates a new master client base.
   *
   * @param clientConf master client configuration
   * @param selectionPolicy master selection policy: which master the client should connect to
   */
  public AbstractMasterClient(
      MasterClientContext clientConf, MasterSelectionPolicy selectionPolicy) {
    super(clientConf);
    mMasterInquireClient = clientConf.getMasterInquireClient();
    mMasterSelectionPolicy = selectionPolicy;
  }

  /**
   * Creates a new master client without a specific address.
   * The client defaults to connect to the primary master.
   * @param clientConf master client configuration
   * @param retryPolicySupplier retry policy to use
   */
  public AbstractMasterClient(
      MasterClientContext clientConf, Supplier<RetryPolicy> retryPolicySupplier) {
    super(clientConf, retryPolicySupplier);
    mMasterInquireClient = clientConf.getMasterInquireClient();
    mMasterSelectionPolicy = MasterSelectionPolicy.Factory.primaryMaster();
  }

  @Override
  public synchronized InetSocketAddress getConfAddress() throws UnavailableException {
    return mMasterSelectionPolicy.getPrimaryMasterAddressCached(mMasterInquireClient);
  }

  @Override
  protected void beforeConnect() throws IOException {
    mMasterSelectionPolicy.resetPrimaryMasterAddressCache();
    super.beforeConnect();
  }

  @Override
  protected void afterDisconnect() {
    super.afterDisconnect();
    mMasterSelectionPolicy.resetPrimaryMasterAddressCache();
  }

  @Override
  protected synchronized GrpcServerAddress queryGrpcServerAddress() throws UnavailableException {
    return GrpcServerAddress.create(
        mMasterSelectionPolicy.getGrpcMasterAddress(mMasterInquireClient));
  }
}

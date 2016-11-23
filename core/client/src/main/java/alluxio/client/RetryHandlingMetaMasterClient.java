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

package alluxio.client;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.MetaMasterClientService;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the meta master.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingMetaMasterClient extends AbstractMasterClient
    implements MetaMasterClient {
  private MetaMasterClientService.Client mClient;

  /**
   * Creates a new block master client.
   *
   * @param masterAddress the master address
   */
  public RetryHandlingMetaMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new MetaMasterClientService.Client(mProtocol);
  }

  /**
   * {@inheritDoc}
   */
  public synchronized MasterInfo getInfo(final List<MasterInfoField> fields)
      throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<MasterInfo>() {
      @Override
      public MasterInfo call() throws TException {
        List<alluxio.thrift.MasterInfoField> thriftFields = new ArrayList<>();
        for (MasterInfoField field : fields) {
          thriftFields.add(field.toThrift());
        }
        return MasterInfo.fromThrift(mClient.getInfo(thriftFields));
      }
    });
  }
}

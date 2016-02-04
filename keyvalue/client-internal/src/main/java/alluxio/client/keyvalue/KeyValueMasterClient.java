/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.keyvalue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import alluxio.Constants;
import alluxio.MasterClientBase;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.thrift.KeyValueMasterClientService;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;

/**
 * A wrapper for the thrift client to interact with the key-value master, used by Tachyon clients.
 * This wrapper provides thread safety, and retry mechanism.
 */
@ThreadSafe
public final class KeyValueMasterClient extends MasterClientBase {
  private KeyValueMasterClientService.Client mClient = null;

  /**
   * Creates a new key-value master client.
   *
   * @param masterAddress the master address
   * @param configuration the Tachyon configuration
   */
  public KeyValueMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new KeyValueMasterClientService.Client(mProtocol);
  }

  /**
   * Marks a partition complete and adds it to an incomplete key-value store.
   *
   * @param path URI of the key-value store
   * @param info information of this completed parition
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completePartition(final AlluxioURI path, final PartitionInfo info)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.completePartition(path.getPath(), info);
        return null;
      }
    });
  }

  /**
   * Marks a key-value store complete.
   *
   * @param path URI of the key-value store
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completeStore(final AlluxioURI path)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.completeStore(path.getPath());
        return null;
      }
    });
  }

  /**
   * Creates a new key-value store.
   *
   * @param path URI of the key-value store
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void createStore(final AlluxioURI path) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.createStore(path.getPath());
        return null;
      }
    });
  }

  /**
   * Gets a list of partitions of a given key-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   * @throws AlluxioException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<PartitionInfo> getPartitionInfo(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsTachyonTException<List<PartitionInfo>>() {
      @Override
      public List<PartitionInfo> call() throws AlluxioTException, TException {
        return mClient.getPartitionInfo(path.getPath());
      }
    });
  }

  /**
   * Deletes a completed key-value store.
   *
   * @param path URI of the store
   * @throws IOException if non-Tachyon error occurs
   * @throws AlluxioException if other Tachyon error occurs
   */
  public synchronized void deleteStore(final AlluxioURI path) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.deleteStore(path.getPath());
        return null;
      }
    });
  }

  /**
   * Merges one completed key-value store to another completed key-value store.
   *
   * @param fromPath URI of the store to be merged
   * @param toPath URI of the store to be merged to
   */
  void mergeStore(final AlluxioURI fromPath, final AlluxioURI toPath)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.mergeStore(fromPath.getPath(), toPath.getPath());
        return null;
      }
    });
  }
}

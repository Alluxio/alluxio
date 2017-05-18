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

package alluxio.client.keyvalue;

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.CompletePartitionTOptions;
import alluxio.thrift.CompleteStoreTOptions;
import alluxio.thrift.CreateStoreTOptions;
import alluxio.thrift.DeleteStoreTOptions;
import alluxio.thrift.GetPartitionInfoTOptions;
import alluxio.thrift.KeyValueMasterClientService;
import alluxio.thrift.MergeStoreTOptions;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.RenameStoreTOptions;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the key-value master, used by Alluxio clients.
 * This wrapper provides thread safety, and retry mechanism.
 */
@ThreadSafe
public final class KeyValueMasterClient extends AbstractMasterClient {
  private KeyValueMasterClientService.Client mClient = null;

  /**
   * Creates a new key-value master client.
   *
   * @param masterAddress the master address
   */
  public KeyValueMasterClient(InetSocketAddress masterAddress) {
    super(null, masterAddress);
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
   * @param info information of this completed partition
   */
  public synchronized void completePartition(final AlluxioURI path, final PartitionInfo info)
      throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.completePartition(path.getPath(), info, new CompletePartitionTOptions());
        return null;
      }
    });
  }

  /**
   * Marks a key-value store complete.
   *
   * @param path URI of the key-value store
   */
  public synchronized void completeStore(final AlluxioURI path) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.completeStore(path.getPath(), new CompleteStoreTOptions());
        return null;
      }
    });
  }

  /**
   * Creates a new key-value store.
   *
   * @param path URI of the key-value store
   */
  public synchronized void createStore(final AlluxioURI path) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.createStore(path.getPath(), new CreateStoreTOptions());
        return null;
      }
    });
  }

  /**
   * Gets a list of partitions of a given key-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   */
  public synchronized List<PartitionInfo> getPartitionInfo(final AlluxioURI path)
      throws IOException {
    return retryRPC(new RpcCallable<List<PartitionInfo>>() {
      @Override
      public List<PartitionInfo> call() throws TException {
        return mClient.getPartitionInfo(path.getPath(), new GetPartitionInfoTOptions())
            .getPartitionInfo();
      }
    });
  }

  /**
   * Deletes a completed key-value store.
   *
   * @param path URI of the store
   */
  public synchronized void deleteStore(final AlluxioURI path) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.deleteStore(path.getPath(), new DeleteStoreTOptions());
        return null;
      }
    });
  }

  /**
   * Renames a completed key-value store.
   *
   * @param oldPath old URI of the store
   * @param newPath new URI of the store
   */
  public synchronized void renameStore(final AlluxioURI oldPath, final AlluxioURI newPath)
      throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.renameStore(oldPath.getPath(), newPath.getPath(), new RenameStoreTOptions());
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
  void mergeStore(final AlluxioURI fromPath, final AlluxioURI toPath) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.mergeStore(fromPath.getPath(), toPath.getPath(), new MergeStoreTOptions());
        return null;
      }
    });
  }
}

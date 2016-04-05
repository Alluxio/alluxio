/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.KeyValueMasterClientService;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.ThriftIOException;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for key-value master RPCs invoked by an Alluxio client.
 */
@ThreadSafe
public final class KeyValueMasterClientServiceHandler implements KeyValueMasterClientService.Iface {
  private final KeyValueMaster mKeyValueMaster;

  /**
   * Constructs the service handler to process incoming RPC calls for key-value master.
   *
   * @param keyValueMaster handler to the real {@link KeyValueMaster} instance
   */
  public KeyValueMasterClientServiceHandler(KeyValueMaster keyValueMaster) {
    mKeyValueMaster = keyValueMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void completePartition(final String path, final PartitionInfo info)
      throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mKeyValueMaster.completePartition(new AlluxioURI(path), info);
        return null;
      }
    });
  }

  @Override
  public void createStore(final String path) throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mKeyValueMaster.createStore(new AlluxioURI(path));
        return null;
      }
    });
  }

  @Override
  public void completeStore(final String path) throws AlluxioTException {
    RpcUtils.call(new RpcCallable<Void>() {
      @Override
      public Void call() throws AlluxioException {
        mKeyValueMaster.completeStore(new AlluxioURI(path));
        return null;
      }
    });
  }

  @Override
  public List<PartitionInfo> getPartitionInfo(final String path) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<List<PartitionInfo>>() {
      @Override
      public List<PartitionInfo> call() throws AlluxioException {
        return mKeyValueMaster.getPartitionInfo(new AlluxioURI(path));
      }
    });
  }

  @Override
  public void deleteStore(final String path) throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mKeyValueMaster.deleteStore(new AlluxioURI(path));
        return null;
      }
    });
  }

  @Override
  public void renameStore(final String oldPath, final String newPath)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mKeyValueMaster.renameStore(new AlluxioURI(oldPath), new AlluxioURI(newPath));
        return null;
      }
    });
  }

  @Override
  public void mergeStore(final String fromPath, final String toPath)
      throws AlluxioTException, ThriftIOException {
    RpcUtils.call(new RpcCallableThrowsIOException<Void>() {
      @Override
      public Void call() throws AlluxioException, IOException {
        mKeyValueMaster.mergeStore(new AlluxioURI(fromPath), new AlluxioURI(toPath));
        return null;
      }
    });
  }
}

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
  public void completePartition(String path, PartitionInfo info) throws AlluxioTException {
    try {
      mKeyValueMaster.completePartition(new AlluxioURI(path), info);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void createStore(String path) throws AlluxioTException {
    try {
      mKeyValueMaster.createStore(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void completeStore(String path) throws AlluxioTException {
    try {
      mKeyValueMaster.completeStore(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public List<PartitionInfo> getPartitionInfo(String path) throws AlluxioTException {
    try {
      return mKeyValueMaster.getPartitionInfo(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void deleteStore(String path) throws AlluxioTException, ThriftIOException {
    try {
      mKeyValueMaster.deleteStore(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void renameStore(String oldPath, String newPath)
      throws AlluxioTException, ThriftIOException {
    try {
      mKeyValueMaster.renameStore(new AlluxioURI(oldPath), new AlluxioURI(newPath));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public void mergeStore(String fromPath, String toPath)
      throws AlluxioTException, ThriftIOException {
    try {
      mKeyValueMaster.mergeStore(new AlluxioURI(fromPath), new AlluxioURI(toPath));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }
}

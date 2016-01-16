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

package tachyon.client.table;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.RawTableMasterClientService;
import tachyon.thrift.RpcOptions;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;
import tachyon.util.IdUtils;

/**
 * A wrapper for the thrift client to interact with the raw table master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RawTableMasterClient extends MasterClientBase {
  private RawTableMasterClientService.Client mClient = null;

  /**
   * Creates a new raw table master client.
   *
   * @param masterAddress the master address
   * @param tachyonConf the Tachyon configuration
   */
  public RawTableMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf);
  }

  @Override
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.RAW_TABLE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.RAW_TABLE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new RawTableMasterClientService.Client(mProtocol);
  }

  /**
   * Creates a raw table. A table is a directory with sub-directories representing columns.
   *
   * @param path the path where the table is placed
   * @param columns the number of columns in the table, must be in range (0, tachyon.max.columns)
   * @param metadata additional metadata about the table, cannot be null
   * @return the id of the table
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized long createRawTable(final TachyonURI path, final int columns,
      final ByteBuffer metadata) throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        RpcOptions rpcOptions = new RpcOptions().setKey(IdUtils.createRpcId());
        return mClient.createRawTable(rpcOptions, path.getPath(), columns, metadata);
      }
    });
  }

  /**
   * Gets the {@link RawTableInfo} associated with the given id.
   *
   * @param id the id of the table
   * @return the table info
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public synchronized RawTableInfo getClientRawTableInfo(final long id)
      throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<RawTableInfo>() {
      @Override
      public RawTableInfo call() throws TachyonTException, TException {
        return mClient.getClientRawTableInfoById(id);
      }
    });
  }

  /**
   * Gets the {@link RawTableInfo} associated with the given path.
   *
   * @param path the path of the table
   * @return the table info
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException when operation fails
   */
  public synchronized RawTableInfo getClientRawTableInfo(final TachyonURI path)
      throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<RawTableInfo>() {
      @Override
      public RawTableInfo call() throws TachyonTException, TException {
        return mClient.getClientRawTableInfoByPath(path.getPath());
      }
    });
  }

  /**
   * Updates the metadata of a table.
   *
   * @param tableId The id of the table to update
   * @param metadata The new metadata to update the table with
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException when operation fails
   */
  public void updateRawTableMetadata(final long tableId, final ByteBuffer metadata)
      throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.updateRawTableMetadata(tableId, metadata);
        return null;
      }
    });
  }
}

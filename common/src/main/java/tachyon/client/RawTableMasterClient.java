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

package tachyon.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.RawTableMasterService;

/**
 * A wrapper for the thrift client to interact with the raw table master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class RawTableMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private RawTableMasterService.Client mClient = null;

  /**
   * Creates a new raw table master client.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public RawTableMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.RAW_TABLE_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new RawTableMasterService.Client(mProtocol);
  }

  /**
   * Creates a raw table. A table is a directory with sub-directories representing columns.
   *
   * @param path the path where the table is placed
   * @param columns the number of columns in the table, must be in range (0, tachyon.max.columns)
   * @param metadata additional metadata about the table, cannot be null
   * @return the id of the table
   * @throws IOException when creation fails
   */
  public synchronized long createRawTable(final TachyonURI path, final int columns,
      final ByteBuffer metadata) throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.createRawTable(path.getPath(), columns, metadata);
      }
    });
  }

  /**
   * Gets the {@link RawTableInfo} associated with the given id.
   *
   * @param id the id of the table
   * @return the table info
   * @throws IOException when operation fails
   */
  public synchronized RawTableInfo getClientRawTableInfo(final long id) throws IOException {
    return retryRPC(new RpcCallable<RawTableInfo>() {
      @Override
      public RawTableInfo call() throws TException {
        return mClient.getClientRawTableInfoById(id);
      }
    });
  }

  /**
   * Gets the {@link RawTableInfo} associated with the given path.
   *
   * @param path the path of the table
   * @return the table info
   * @throws IOException when operation fails
   */
  public synchronized RawTableInfo getClientRawTableInfo(final TachyonURI path) throws IOException {
    return retryRPC(new RpcCallable<RawTableInfo>() {
      @Override
      public RawTableInfo call() throws TException {
        return mClient.getClientRawTableInfoByPath(path.getPath());
      }
    });
  }

  /**
   * Updates the metadata of a table.
   *
   * @param tableId The id of the table to update
   * @param metadata The new metadata to update the table with
   * @throws IOException when the operation fails
   */
  public void updateRawTableMetadata(final long tableId, final ByteBuffer metadata)
      throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.updateRawTableMetadata(tableId, metadata);
        return null;
      }
    });
  }

  // TODO(cc) {@link #RpcCallable} and {@link #retryPRC}  may be valuable to other clients too, but
  // let's limit its use to this class in this PR, may be replaced if a better solution is found.
  /**
   * The RPC to be executed in {@link #retryRPC(RpcCallable)}.
   *
   * @param <V> the return value of {@link #call()}
   */
  private interface RpcCallable<V> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws TException when any exception defined in thrift happens
     */
    V call() throws TException;
  }

  /**
   * Tries to execute a RPC defined as a {@link RpcCallable}, if error
   * happens in one execution, a reconnection will be tried through {@link #connect()} and the
   * action will be re-executed.
   *
   * @param rpc the {@link RpcCallable} to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY} or {@link #close()} has
   *                     been called before calling this method or during the retry
   */
  private <V> V retryRPC(RpcCallable<V> rpc) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return rpc.call();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}

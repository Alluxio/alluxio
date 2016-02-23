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

package alluxio.client.keyvalue;

import alluxio.AbstractClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.KeyValueWorkerClientService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client for talking to a key-value worker server.
 *
 * Since {@link KeyValueWorkerClientService.Client} is not thread safe, this class has to guarantee
 * thread safety.
 */
@ThreadSafe
public final class KeyValueWorkerClient extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KeyValueWorkerClientService.Client mClient = null;

  /**
   * Creates a {@link KeyValueWorkerClient}.
   *
   * @param workerNetAddress location of the worker to connect to
   * @param conf Alluxio configuration
   */
  public KeyValueWorkerClient(WorkerNetAddress workerNetAddress, Configuration conf) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), conf,
        "key-value-worker");
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.KEY_VALUE_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.KEY_VALUE_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new KeyValueWorkerClientService.Client(mProtocol);
  }

  /**
   * Gets the value of a given {@code key} from a specific key-value block.
   *
   * @param blockId The id of the block
   * @param key the key to get the value for
   * @return ByteBuffer of value, or null if not found
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  public synchronized ByteBuffer get(final long blockId, final ByteBuffer key)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws AlluxioTException, TException {
        return mClient.get(blockId, key);
      }
    });
  }

  /**
   * Gets a batch of keys next to the current key in the partition.
   * <p>
   * If current key is null, it means get the initial batch of keys.
   * If there are no more next keys, an empty list is returned.
   *
   * @param blockId the id of the partition
   * @param key the current key
   * @param numKeys maximum number of next keys to fetch
   * @return the next batch of keys
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  public synchronized List<ByteBuffer> getNextKeys(final long blockId, final ByteBuffer key,
      final int numKeys) throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<ByteBuffer>>() {
      @Override
      public List<ByteBuffer> call() throws AlluxioTException, TException {
        return mClient.getNextKeys(blockId, key, numKeys);
      }
    });
  }

  /**
   * @param blockId the id of the partition
   * @return the number of key-value pairs in the partition
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AlluxioException if an exception in Alluxio occurs
   */
  public synchronized int getSize(final long blockId) throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Integer>() {
      @Override
      public Integer call() throws AlluxioTException, TException {
        return mClient.getSize(blockId);
      }
    });
  }
}

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

package tachyon.client.keyvalue;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.ClientBase;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.KeyValueWorkerClientService;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.NetAddress;

/**
 * Client for talking to a key-value worker server.
 *
 * Since {@link KeyValueWorkerClientService.Client} is not thread safe, this class has to guarantee
 * thread safety.
 */
@ThreadSafe
public final class KeyValueWorkerClient extends ClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KeyValueWorkerClientService.Client mClient = null;

  /**
   * Creates a {@link KeyValueWorkerClient}.
   *
   * @param workerNetAddress location of the worker to connect to
   * @param conf Tachyon configuration
   */
  public KeyValueWorkerClient(WorkerNetAddress workerNetAddress, TachyonConf conf) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(new NetAddress(workerNetAddress)), conf,
        "key-value-worker");
  }

  @Override
  protected TachyonService.Client getClient() {
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
   * @return ByteBuffer of value, or null if not found
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized ByteBuffer get(final long blockId, final ByteBuffer key)
      throws IOException, TachyonException {
    return retryRPC(new ClientBase.RpcCallableThrowsTachyonTException<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws TachyonTException, TException {
        return mClient.get(blockId, key);
      }
    });
  }
}

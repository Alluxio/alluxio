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

package tachyon.worker.lineage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.ClientBase;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterWorkerService;
import tachyon.thrift.TachyonService;

/**
 * A wrapper for the thrift client to interact with the lineage master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class LineageMasterClient extends ClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private LineageMasterWorkerService.Client mClient = null;

  /**
   * @param masterAddress the master address
   * @param tachyonConf tachyonConf
   */
  public LineageMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf, "lineage-worker");
  }

  @Override
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.LINEAGE_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.LINEAGE_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new LineageMasterWorkerService.Client(mProtocol);
  }

  /**
   * Instructs a worker to persist the files for checkpoint.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the persisted files
   * @return the command for checkpointing the blocks of a file
   * @throws IOException if file persistence fails
   */
  public synchronized LineageCommand heartbeat(final long workerId,
      final List<Long> persistedFiles) throws IOException {
    return retryRPC(new RpcCallable<LineageCommand>() {
      @Override
      public LineageCommand call() throws TException {
        return mClient.heartbeat(workerId, persistedFiles);
      }
    });
  }
}

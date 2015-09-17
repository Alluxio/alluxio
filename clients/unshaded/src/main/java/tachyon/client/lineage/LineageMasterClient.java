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

package tachyon.client.lineage;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.client.file.TachyonFile;
import tachyon.conf.TachyonConf;
import tachyon.job.Job;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterService;

/**
 * A wrapper for the thrift client to interact with the lineage master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class LineageMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private LineageMasterService.Client mClient = null;

  /**
   * Creates a new lineage master client.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public LineageMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.LINEAGE_MASTER_SERVICE_NAME;
  }

  public synchronized long addLineage(List<TachyonFile> inputFiles, List<TachyonFile> outputFiles,
      Job job) {
    // TODO add lineage
    return -1;
  }

  public synchronized boolean deleteLineage(long lineageId) {
    // TODO delete lineage
    return false;
  }


  /**
   * Instructs a worker to persist the files for checkpoint.
   *
   * @param workerId the id of the worker that heartbeats
   * @return the command for checkpointing the blocks of a file.
   */
  public synchronized LineageCommand lineageWorkerHeartbeat(long workerId) {
    // TODO
    return null;
  }
}

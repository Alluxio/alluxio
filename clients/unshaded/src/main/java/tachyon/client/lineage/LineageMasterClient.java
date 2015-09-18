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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.job.Job;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.LineageMasterService;
import tachyon.util.io.SerDeUtils;

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

  @Override
  protected void afterConnect() {
    mClient = new LineageMasterService.Client(mProtocol);
  }


  public synchronized long addLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles,
      Job job) throws IOException, FileDoesNotExistException {
    // prepare for RPC
    List<String> inputFileStrings = Lists.newArrayList();
    for (TachyonURI inputFile : inputFiles) {
      inputFileStrings.add(inputFile.toString());
    }
    List<String> outputFileStrings = Lists.newArrayList();
    for (TachyonURI outputFile : outputFiles) {
      outputFileStrings.add(outputFile.toString());
    }
    byte[] jobByteArray = SerDeUtils.objectToByteArray(job);

    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.createLineage(inputFileStrings, outputFileStrings,
            ByteBuffer.wrap(jobByteArray));
      } catch (FileDoesNotExistException e) {
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  public synchronized boolean deleteLineage(long lineageId, boolean cascade) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.deleteLineage(lineageId, cascade);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}

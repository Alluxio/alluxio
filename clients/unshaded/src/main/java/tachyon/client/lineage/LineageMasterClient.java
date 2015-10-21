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
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.thrift.LineageInfo;
import tachyon.thrift.LineageMasterService;
import tachyon.thrift.TachyonTException;

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
   * @param tachyonConf the Tachyon configuration
   */
  public LineageMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.LINEAGE_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new LineageMasterService.Client(mProtocol);
  }

  public synchronized long createLineage(final List<String> inputFiles,
      final List<String> outputFiles, final CommandLineJob job) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.createLineage(inputFiles, outputFiles,
            job.generateCommandLineJobInfo());
      }
    });
  }

  public synchronized boolean deleteLineage(final long lineageId, final boolean cascade)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.deleteLineage(lineageId, cascade);
      }
    });
  }

  public synchronized long reinitializeFile(final String path, final long blockSizeBytes,
      final long ttl) throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.reinitializeFile(path, blockSizeBytes, ttl);
      }
    });
  }

  public synchronized void asyncCompleteFile(final long fileId) throws IOException {
    try {
      retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
        @Override
        public Void call() throws TachyonTException, TException {
          mClient.asyncCompleteFile(fileId);
          return null;
        }
      });
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized List<LineageInfo> getLineageInfoList() throws IOException {
    return retryRPC(new RpcCallable<List<LineageInfo>>() {
      @Override
      public List<LineageInfo> call() throws TException {
        return mClient.getLineageInfoList();
      }
    });
  }
}

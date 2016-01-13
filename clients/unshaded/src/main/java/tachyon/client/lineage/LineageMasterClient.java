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
import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.thrift.LineageInfo;
import tachyon.thrift.LineageMasterClientService;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;

/**
 * A wrapper for the thrift client to interact with the lineage master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class LineageMasterClient extends MasterClientBase {
  private LineageMasterClientService.Client mClient = null;

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
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new LineageMasterClientService.Client(mProtocol);
  }

  /**
   * Creates a lineage.
   *
   * @param inputFiles the list of input file names
   * @param outputFiles the list of output file names
   * @param job the job used for the creation
   * @return the value of the lineage creation result
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
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

  /**
   * Deletes a lineage.
   *
   * @param lineageId the id of the lineage
   * @param cascade true if the deletion is cascading, false otherwise
   * @return true if the deletion was successful, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public synchronized boolean deleteLineage(final long lineageId, final boolean cascade)
      throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.deleteLineage(lineageId, cascade);
      }
    });
  }

  /**
   * Reinitializates a given file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the size of the block in bytes
   * @param ttl the time to live for the file
   * @return the value of the lineage creation result
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public synchronized long reinitializeFile(final String path, final long blockSizeBytes,
      final long ttl) throws IOException, TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Long>() {
      @Override
      public Long call() throws TachyonTException, TException {
        return mClient.reinitializeFile(path, blockSizeBytes, ttl);
      }
    });
  }

  /**
   * Retrieves the list of lineage information.
   *
   * @return a list of lineage information
   * @throws ConnectionFailedException if the connection fails
   * @throws IOException if a non-Tachyon exception occurs
   */
  public synchronized List<LineageInfo> getLineageInfoList()
      throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<List<LineageInfo>>() {
      @Override
      public List<LineageInfo> call() throws TException {
        return mClient.getLineageInfoList();
      }
    });
  }

  /**
   * Reports a file as lost.
   *
   * @param path the path to the lost file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public synchronized void reportLostFile(final String path) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.reportLostFile(path);
        return null;
      }
    });
  }
}

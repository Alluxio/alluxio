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

package alluxio.client.lineage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import alluxio.Constants;
import alluxio.MasterClientBase;
import alluxio.Configuration;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.thrift.LineageMasterClientService;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.wire.LineageInfo;
import alluxio.wire.ThriftUtils;

/**
 * A wrapper for the thrift client to interact with the lineage master, used by alluxio clients.
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
   * @param configuration the Alluxio configuration
   */
  public LineageMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
  }

  @Override
  protected AlluxioService.Client getClient() {
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
   * @throws IOException if a non-Alluxio exception occurs
   * @throws AlluxioException if a Alluxio exception occurs
   */
  public synchronized long createLineage(final List<String> inputFiles,
      final List<String> outputFiles, final CommandLineJob job) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.createLineage(inputFiles, outputFiles,
            ThriftUtils.toThrift(job.generateCommandLineJobInfo()));
      }
    });
  }

  /**
   * Deletes a lineage.
   *
   * @param lineageId the id of the lineage
   * @param cascade true if the deletion is cascading, false otherwise
   * @return true if the deletion was successful, false otherwise
   * @throws IOException if a non-Alluxio exception occurs
   * @throws AlluxioException if a Alluxio exception occurs
   */
  public synchronized boolean deleteLineage(final long lineageId, final boolean cascade)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
      @Override
      public Boolean call() throws AlluxioTException, TException {
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
   * @throws IOException if a non-Alluxio exception occurs
   * @throws LineageDoesNotExistException if the file does not exist
   * @throws AlluxioException if a Alluxio exception occurs
   */
  public synchronized long reinitializeFile(final String path, final long blockSizeBytes,
      final long ttl) throws IOException, LineageDoesNotExistException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.reinitializeFile(path, blockSizeBytes, ttl);
      }
    });
  }

  /**
   * Retrieves the list of lineage information.
   *
   * @return a list of lineage information
   * @throws ConnectionFailedException if the connection fails
   * @throws IOException if a non-Alluxio exception occurs
   */
  public synchronized List<LineageInfo> getLineageInfoList()
      throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<List<LineageInfo>>() {
      @Override
      public List<LineageInfo> call() throws TException {
        List<LineageInfo> result = new ArrayList<LineageInfo>();
        for (alluxio.thrift.LineageInfo lineageInfo : mClient.getLineageInfoList()) {
          result.add(ThriftUtils.fromThrift(lineageInfo));
        }
        return result;
      }
    });
  }

  /**
   * Reports a file as lost.
   *
   * @param path the path to the lost file
   * @throws IOException if a non-Alluxio exception occurs
   * @throws AlluxioException if a Alluxio exception occurs
   */
  public synchronized void reportLostFile(final String path) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.reportLostFile(path);
        return null;
      }
    });
  }
}

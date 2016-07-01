/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.CommandLineJob;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.LineageMasterClientService;
import alluxio.wire.LineageInfo;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the lineage master, used by alluxio clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class LineageMasterClient extends AbstractMasterClient {
  private LineageMasterClientService.Client mClient = null;

  /**
   * Creates a new lineage master client.
   *
   * @param masterAddress the master address
   */
  public LineageMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress);
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
   * @throws AlluxioException if an Alluxio exception occurs
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
   * @throws AlluxioException if an Alluxio exception occurs
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
   * @throws AlluxioException if an Alluxio exception occurs
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
        List<LineageInfo> result = new ArrayList<>();
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
   * @throws AlluxioException if an Alluxio exception occurs
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

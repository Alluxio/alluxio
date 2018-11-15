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

package alluxio.master.file;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.master.MasterClientConfig;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemMasterJobService;
import alluxio.thrift.GetUfsInfoTOptions;
import alluxio.thrift.UfsInfo;
import alluxio.wire.FileInfo;

import org.apache.thrift.TException;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the file system master, used by Alluxio job
 * service.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class FileSystemMasterClient extends AbstractMasterClient {
  private FileSystemMasterJobService.Client mClient = null;

  /**
   * Creates an instance of {@link FileSystemMasterClient}.
   *
   * @param conf master client configuration
   */
  public FileSystemMasterClient(MasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new FileSystemMasterJobService.Client(mProtocol);
  }

  /**
   * @param fileId the id of the file for which to get the {@link FileInfo}
   * @return the file info for the given file id
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException {
    return retryRPC(new RpcCallable<FileInfo>() {
      @Override
      public FileInfo call() throws TException {
        return null;
      }
    });
  }

  /**
   * @param mountId the id of the mount
   * @return the ufs information for the give ufs
   * @throws IOException if an I/O error occurs
   */
  public synchronized UfsInfo getUfsInfo(final long mountId) throws IOException {
    return retryRPC(new RpcCallable<UfsInfo>() {
      @Override
      public UfsInfo call() throws TException {
        return mClient.getUfsInfo(mountId, new GetUfsInfoTOptions()).getUfsInfo();
      }
    });
  }
}

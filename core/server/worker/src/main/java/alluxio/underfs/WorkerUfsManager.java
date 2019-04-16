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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.UfsInfo;
import alluxio.master.MasterClientContext;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The default implementation of UfsManager to manage the ufs used by different worker services.
 */
@ThreadSafe
public final class WorkerUfsManager extends AbstractUfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerUfsManager.class);

  private final FileSystemMasterClient mMasterClient;

  /**
   * Constructs an instance of {@link WorkerUfsManager}.
   */
  public WorkerUfsManager() {
    mMasterClient = mCloser.register(new FileSystemMasterClient(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build()));
  }

  /**
   * {@inheritDoc}.
   *
   * If this mount id is new to this worker, this method will query master to get the
   * corresponding ufs info.
   */
  @Override
  public UfsClient get(long mountId) throws NotFoundException, UnavailableException {
    try {
      return super.get(mountId);
    } catch (NotFoundException e) {
      // Not cached locally, let's query master
    }

    UfsInfo info;
    try {
      info = mMasterClient.getUfsInfo(mountId);
    } catch (IOException e) {
      throw new UnavailableException(
          String.format("Failed to create UFS info for mount point with id %d", mountId), e);
    }
    Preconditions.checkState((info.hasUri() && info.hasProperties()), "unknown mountId");
    super.addMount(mountId, new AlluxioURI(info.getUri()),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
            .setReadOnly(info.getProperties().getReadOnly())
            .setShared(info.getProperties().getShared())
            .createMountSpecificConf(info.getProperties().getPropertiesMap()));
    UfsClient ufsClient = super.get(mountId);
    return ufsClient;
  }

  @Override
  protected void connectUfs(UnderFileSystem fs) throws IOException {
    fs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC,
            ServerConfiguration.global()));
  }
}

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
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.UfsInfo;
import alluxio.master.MasterClientConfig;
import alluxio.master.file.FileSystemMasterClient;
import alluxio.resource.CloseableResource;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UfsManager to manage the ufs used by different job service processes.
 */
// TODO(jiri): Avoid duplication of logic with WorkerUfsManager.
@ThreadSafe
public final class JobUfsManager extends AbstractUfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(JobUfsManager.class);

  private final FileSystemMasterClient mMasterClient;

  /**
   * Constructs an instance of {@link JobUfsManager}.
   */
  public JobUfsManager() {
    mMasterClient = mCloser.register(new FileSystemMasterClient(MasterClientConfig.defaults()));
  }

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
          String.format("Failed to get UFS info for mount point with id %d", mountId), e);
    }
    Preconditions.checkState((info.hasUri() && info.hasProperties()), "unknown mountId");
    super.addMount(mountId, new AlluxioURI(info.getUri()),
        UnderFileSystemConfiguration.defaults().setReadOnly(info.getProperties().getReadOnly())
            .setShared(info.getProperties().getShared())
            .setMountSpecificConf(info.getProperties().getPropertiesMap()));
    UfsClient ufsClient = super.get(mountId);
    try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      ufs.connectFromWorker(
          NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    } catch (IOException e) {
      removeMount(mountId);
      throw new UnavailableException(
          String.format("Failed to connect to UFS %s with id %d", info.getUri(), mountId), e);
    }
    return ufsClient;
  }
}

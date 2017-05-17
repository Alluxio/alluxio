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

import alluxio.thrift.UfsInfo;
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
    mMasterClient = mCloser.register(new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC)));
  }

  /**
   * Establishes the connection to the given UFS from worker.
   * @param ufs UFS instance
   * @throws IOException if failed to create the UFS instance
   */
  protected void connect(UnderFileSystem ufs) throws IOException {
    ufs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
  }

  /**
   * {@inheritDoc}.
   *
   * If this mount id is new to this worker, this method will query master to get the corresponding
   * ufs info.
   */
  @Override
  public UnderFileSystem get(long mountId) {
    UnderFileSystem ufs = super.get(mountId);
    if (ufs == null) {
      UfsInfo info;
      try {
        info = mMasterClient.getUfsInfo(mountId);
      } catch (IOException e) {
        LOG.error("Failed to get UFS info for mount point with id {}", mountId);
        return null;
      }
      Preconditions.checkState((info.isSetUri() && info.isSetProperties()), "unknown mountId");
      try {
        ufs = super.addMount(mountId, info.getUri(), info.getProperties());
      } catch (IOException e) {
        LOG.error("Failed to add mount point {} with id {}", info.getUri(), mountId, e);
        return null;
      }
    }
    return ufs;
  }
}

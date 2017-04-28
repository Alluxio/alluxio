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

package alluxio.worker;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.thrift.UfsInfo;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The default implementation of UfsManager to manage the ufs used by different worker services.
 */
@ThreadSafe
public final class WorkerUfsManager extends UfsManager {

  private final FileSystemMasterClient mMasterClient;

  /**
   * Constructs an instance of {@link WorkerUfsManager}.
   */
  public WorkerUfsManager() {
    mMasterClient = mCloser.register(new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC)));
  }

  /**
   * {@inheritDoc}.
   *
   * If this mount id is new to this worker, this method will query master to get the corresponding
   * ufs info.
   */
  public UnderFileSystem get(long mountId) {
    UnderFileSystem ufs = super.get(mountId);
    if (ufs == null) {
      UfsInfo info;
      try {
        info = mMasterClient.getUfsInfo(mountId);
      } catch (IOException e) {
        throw AlluxioStatusException.fromIOException(e);
      }
      Preconditions.checkState((info.isSetUri() && info.isSetProperties()), "unknown mountId");
      ufs = super.addMount(mountId, info.getUri(), info.getProperties());
    }
    return ufs;
  }
}

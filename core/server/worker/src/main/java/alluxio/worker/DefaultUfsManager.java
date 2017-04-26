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

import alluxio.exception.AlluxioException;
import alluxio.thrift.UfsInfo;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;

/**
 * The default implementation of UfsManager to manage the ufs used by different worker services.
 */
public final class DefaultUfsManager implements UfsManager {

  private final Object mLock = new Object();

  private final FileSystemMasterClient mMasterClient;
  private final Closer mCloser;

  /**
   * Constructs an instance of {@link DefaultUfsManager}.
   */
  public DefaultUfsManager() {
    mCloser = Closer.create();
    mMasterClient = mCloser.register(new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC)));
  }

  @Override
  public UnderFileSystem getUfsByMountId(long mountId) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(mountId);
    if (ufs == null) {
      UfsInfo info;
      try {
        info = mMasterClient.getUfsInfo(mountId);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
      Preconditions.checkState((info.isSetUri() && info.isSetProperties()));
      ufs = UnderFileSystem.Factory.get(info.getUri(), info.getProperties(), mountId);
      mCloser.register(ufs);
    }
    return ufs;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}

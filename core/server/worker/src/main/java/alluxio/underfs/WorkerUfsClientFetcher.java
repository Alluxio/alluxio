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
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientConfig;
import alluxio.underfs.DefaultUfsClientCache.UfsClientFetcher;
import alluxio.worker.UfsClientCache.UfsClient;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The default implementation of UfsManager to manage the ufs used by different worker services.
 */
@ThreadSafe
public class WorkerUfsClientFetcher implements UfsClientFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerUfsClientFetcher.class);

  private final FileSystemMasterClient mMasterClient;
  private final UfsCache mUfsCache;

  private final Closer mCloser;

  /**
   * Constructs an instance of {@link WorkerUfsClientFetcher}.
   */
  public WorkerUfsClientFetcher(UfsCache ufsCache) {
    mCloser = Closer.create();
    mUfsCache = ufsCache;
    mMasterClient = mCloser.register(new FileSystemMasterClient(MasterClientConfig.defaults()));
  }

  @Override
  public UfsClient getClient(long mountId) throws IOException {
    alluxio.thrift.UfsInfo info;
    try {
      info = mMasterClient.getUfsInfo(mountId);
    } catch (IOException e) {
      throw new UnavailableException(
          String.format("Failed to get UFS info for mount point with id %d", mountId), e);
    }
    Preconditions.checkState((info.isSetUri() && info.isSetProperties()), "unknown mountId");
    AlluxioURI ufsUri = new AlluxioURI(info.getUri());

    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults()
        .setReadOnly(info.getProperties().isReadOnly())
        .setShared(info.getProperties().isShared())
        .setMountSpecificConf(info.getProperties().getProperties());
    UnderFileSystem ufs = mUfsCache.getOrAdd(ufsUri, ufsConf);
    ufs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    return new UfsClient(() -> mUfsCache.getOrAdd(ufsUri, ufsConf), ufsUri);
  }
}

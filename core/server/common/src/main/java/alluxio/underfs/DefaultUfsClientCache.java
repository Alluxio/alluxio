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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.util.IdUtils;
import alluxio.worker.UfsClientCache;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic cache for ufs clients.
 */
public class DefaultUfsClientCache implements UfsClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultUfsClientCache.class);

  /**
   * Maps from mount id to {@link UfsClient} instances. This map helps efficiently retrieve
   * existing UFS info given its mount id.
   */
  private final ConcurrentHashMap<Long, UfsClient> mMountIdToUfsInfoMap =
      new ConcurrentHashMap<>();

  private UfsClient mRootUfsClient;

  private UfsCache mUfsCache;
  private UfsClientFetcher mUfsClientFetcher;

  private final Closer mCloser;

  public DefaultUfsClientCache(UfsCache ufsCache, UfsClientFetcher ufsClientFetcher) {
    mUfsCache = ufsCache;
    mUfsClientFetcher = ufsClientFetcher;
    mCloser = Closer.create();
  }

  @Override
  public UfsClient get(long mountId) throws IOException {
    if (mountId == IdUtils.ROOT_MOUNT_ID) {
      return getRoot();
    }
    UfsClient ufsClient = mMountIdToUfsInfoMap.get(mountId);
    if (ufsClient == null) {
      ufsClient = mUfsClientFetcher.getClient(mountId);
      if (ufsClient == null) {
        throw new NotFoundException(
            String.format("Mount Id %d not found in cached mount points", mountId));
      }
      mMountIdToUfsInfoMap.put(mountId, ufsClient);
    }
    return ufsClient;
  }

  @Override
  public UfsClient getRoot() {
    synchronized (this) {
      if (mRootUfsClient == null) {
        AlluxioURI rootUri = new AlluxioURI(Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS));
        boolean rootReadOnly =
            Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
        boolean rootShared = Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED);
        Map<String, String> mountSpecificConf =
            Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
        UnderFileSystemConfiguration rootConf = UnderFileSystemConfiguration.defaults()
            .setReadOnly(rootReadOnly)
            .setShared(rootShared)
            .setMountSpecificConf(mountSpecificConf);
        mRootUfsClient = new UfsClient(() -> mUfsCache.getOrAdd(rootUri, rootConf), rootUri);
      }
      return mRootUfsClient;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * An object which can create UfsClients from mount ids.
   */
  public interface UfsClientFetcher {
    /**
     * @param mountId a mount id
     * @return a ufs client for the specified mount id
     */
    UfsClient getClient(long mountId) throws IOException;
  }
}

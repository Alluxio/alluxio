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

import java.io.Closeable;

/**
 * A class that manages the UFS used by different services.
 */
public interface UfsManager extends Closeable {
  /** Container for a UFS and the URI for that UFS. */
  class UfsInfo {
    private UnderFileSystem mUfs;
    private AlluxioURI mUfsMountPointUri;
    private UnderFileSystemConfiguration mUfsConf;

    /**
     * @param ufsConf a UFS configuration
     * @param ufsMountPointUri the URI for the UFS path which is mounted in Alluxio
     */
    public UfsInfo(UnderFileSystemConfiguration ufsConf, AlluxioURI ufsMountPointUri) {
      mUfsMountPointUri = ufsMountPointUri;
      mUfsConf = ufsConf;
    }

    /**
     * @return the UFS instance initialized in a lazy way
     */
    public UnderFileSystem getUfs() {
      return mUfs;
    }

    /**
     * @return the UFS configuration
     */
    public UnderFileSystemConfiguration getUfsConf() {
      return mUfsConf;
    }

    /**
     * @return the URI for the UFS path which is mounted in Alluxio
     */
    public AlluxioURI getUfsMountPointUri() {
      return mUfsMountPointUri;
    }

    /**
     * @param ufs the UFS instance to set
     */
    public void setUfs(UnderFileSystem ufs) {
      mUfs = ufs;
    }
  }

  /**
   * Keeps track of a mount id and map it to a its URI and configuration. This is an Alluxio-only
   * operation and no interaction to UFS is made.
   *
   * @param mountId the mount id
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   */
  void addMount(long mountId, AlluxioURI ufsUri, UnderFileSystemConfiguration ufsConf);

  /**
   * Removes the association from a mount id to a UFS instance. If the mount id is not known, this
   * is a noop.
   *
   * @param mountId the mount id
   *
   */
  void removeMount(long mountId);

  /**
   * Gets UFS information from the cache if exists, or throws exception otherwise. The UFS
   * information is created lazily on get, based on the UFS uri and conf. If the UFS already exists
   * in the cache, maps the mount id to the existing UFS. Otherwise, creates a new UFS and adds it
   * to the cache.
   *
   * @param mountId the mount id
   * @return the UFS information
   * @throws NotFoundException if mount id is not found in mount table
   * @throws UnavailableException if master is not available to query for mount table
   */
  UfsInfo get(long mountId) throws NotFoundException, UnavailableException;

  /**
   * @return the UFS information associated with root
   */
  UfsInfo getRoot();
}

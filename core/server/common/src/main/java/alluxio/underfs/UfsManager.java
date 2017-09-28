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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import java.io.Closeable;

/**
 * A class that manages the UFS used by different services.
 */
public interface UfsManager extends Closeable {
  /** Container for a UFS and the URI for that UFS. */
  class UfsInfo {
    private UnderFileSystem mUfs;
    private final AlluxioURI mUfsMountPointUri;
    private final Supplier<UnderFileSystem> mUfsSupplier;

    /**
     * @param ufsSupplier the supplier function to create a new UFS instance
     * @param ufsMountPointUri the URI for the UFS path which is mounted in Alluxio
     */
    public UfsInfo(Supplier<UnderFileSystem> ufsSupplier, AlluxioURI ufsMountPointUri) {
      mUfsSupplier = Preconditions.checkNotNull(ufsSupplier, "ufsSupplier is null");
      mUfsMountPointUri = Preconditions.checkNotNull(ufsMountPointUri, "ufsMountPointUri is null");
    }

    /**
     * @return the UFS instance
     */
    public synchronized UnderFileSystem getUfs() {
      if (mUfs == null) {
        mUfs = mUfsSupplier.get();
      }
      return mUfs;
    }

    /**
     * @return the URI for the UFS path which is mounted in Alluxio
     */
    public AlluxioURI getUfsMountPointUri() {
      return mUfsMountPointUri;
    }
  }

  /**
   * Keeps track of a mount id and maps it to its URI in Alluxio and configuration. This is an
   * Alluxio-only operation and no interaction to UFS will be made.
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
   * Gets UFS information from the manager if this mount ID exists, or throws exception otherwise.
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

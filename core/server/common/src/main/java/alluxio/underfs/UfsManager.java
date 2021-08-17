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
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A class that manages the UFS used by different services.
 */
public interface UfsManager extends Closeable {
  /** Container for a UFS and the URI for that UFS. */
  class UfsClient {
    private static final Logger LOG = LoggerFactory.getLogger(UfsClient.class);

    private final AtomicReference<UnderFileSystem> mUfs;
    private final AlluxioURI mUfsMountPointUri;
    private final Supplier<UnderFileSystem> mUfsSupplier;
    private final Counter mCounter;

    /**
     * @param ufsSupplier the supplier function to create a new UFS instance
     * @param ufsMountPointUri the URI for the UFS path which is mounted in Alluxio
     */
    public UfsClient(Supplier<UnderFileSystem> ufsSupplier, AlluxioURI ufsMountPointUri) {
      mUfsSupplier = Preconditions.checkNotNull(ufsSupplier, "ufsSupplier is null");
      mUfsMountPointUri = Preconditions.checkNotNull(ufsMountPointUri, "ufsMountPointUri is null");
      mCounter = MetricsSystem.counter(
          String.format("UfsSessionCount-Ufs:%s", MetricsSystem.escape(mUfsMountPointUri)));
      mUfs = new AtomicReference<>();
    }

    /**
     * @return the UFS instance
     */
    public CloseableResource<UnderFileSystem> acquireUfsResource() {
      if (mUfs.get() == null) {
        UnderFileSystem ufs = mUfsSupplier.get();
        if (!mUfs.compareAndSet(null, ufs)) {
          // Another thread already added this ufs, close this one.
          try {
            ufs.close();
          } catch (IOException e) {
            // ignore the close exception, log a warning
            LOG.warn(String
                .format("Failed to close extra UFS. mount point: %s error: %s", mUfsMountPointUri,
                    e.toString()));
          }
        }
      }
      mCounter.inc();
      return new CloseableResource<UnderFileSystem>(mUfs.get()) {
        @Override
        public void close() {
          mCounter.dec();
        }
      };
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
  UfsClient get(long mountId) throws NotFoundException, UnavailableException;

  /**
   * @return the UFS client associated with root
   */
  UfsClient getRoot();

  /**
   * @param location the journal location
   * @return the UFS client associated with journal location
   */
  UfsClient getJournal(URI location);
}

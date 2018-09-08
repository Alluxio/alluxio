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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.MountOptions;
import alluxio.resource.CloseableResource;
import alluxio.worker.UfsClientCache;
import alluxio.worker.UfsClientCache.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class for resolving Alluxio-space paths into {@link Resolution}s.
 */
public class MountResolver {
  private static final Logger LOG = LoggerFactory.getLogger(MountResolver.class);

  private final MountTable mMountTable;
  private final UfsClientCache mUfsClientCache;

  public MountResolver(MountTable mountTable, UfsClientCache ufsClientCache) {
    mMountTable = mountTable;
    mUfsClientCache = ufsClientCache;
  }

  /**
   * Resolves the given Alluxio path. If the given Alluxio path is nested under a mount point, the
   * resolution maps the Alluxio path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param uri an Alluxio path URI
   * @return the resolution representing the UFS path
   */
  public Resolution resolve(AlluxioURI uri) throws InvalidPathException, IOException {
    String path = uri.getPath();
    LOG.debug("Resolving {}", path);
    // This will re-acquire the read lock, but that is allowed.
    MountInfo mountInfo = mMountTable.getMountInfo(uri);
    if (mountInfo != null) {
      AlluxioURI alluxioUri = mountInfo.getAlluxioUri();
      AlluxioURI ufsUri = mountInfo.getUfsUri();
      UfsClient ufsClient;
      AlluxioURI resolvedUri;
      try {
        ufsClient = mUfsClientCache.get(mountInfo.getMountId());
        try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          resolvedUri = ufs.resolveUri(ufsUri, path.substring(alluxioUri.getPath().length()));
        }
      } catch (NotFoundException | UnavailableException e) {
        throw new RuntimeException(
            String.format("No UFS information for %s for mount Id %d, we should never reach here",
                uri, mountInfo.getMountId()), e);
      }
      return new Resolution(resolvedUri, ufsClient, mountInfo);
    }
    // TODO(binfan): throw exception as we should never reach here
    return new Resolution(uri, null,
        new MountInfo(uri, uri, IdUtils.INVALID_MOUNT_ID, MountOptions.defaults()));
  }


  /**
   * This class represents a UFS path after resolution. The UFS URI and the {@link UnderFileSystem}
   * for the UFS path are available.
   */
  public static final class Resolution {
    private final AlluxioURI mUri;
    private final UfsClient mUfsClient;
    private final MountInfo mMountInfo;

    private Resolution(AlluxioURI uri, UfsClient ufs, MountInfo mountInfo) {
      mUri = uri;
      mUfsClient = ufs;
      mMountInfo = mountInfo;
    }

    /**
     * @return the URI in the ufs
     */
    public AlluxioURI getUri() {
      return mUri;
    }

    /**
     * @return the {@link UnderFileSystem} closeable resource
     */
    public CloseableResource<UnderFileSystem> acquireUfsResource() {
      return mUfsClient.acquireUfsResource();
    }

    /**
     * @return the shared option
     */
    public boolean getShared() {
      return mMountInfo.getOptions().isShared();
    }

    /**
     * @return the id of this mount point
     */
    public long getMountId() {
      return mMountInfo.getMountId();
    }

    /**
     * @return the mount info for the mount point
     */
    public MountInfo getMountInfo() {
      return mMountInfo;
    }
  }
}

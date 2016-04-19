/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.MountOptions;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is used for keeping track of Alluxio mount points.
 */
@ThreadSafe
public final class MountTable {
  public static final String ROOT = "/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Maps from Alluxio path string, to {@link MountInfo}. */
  private Map<String, MountInfo> mMountTable;

  /**
   * Creates a new instance of {@link MountTable}.
   */
  public MountTable() {
    final int initialCapacity = 10;
    mMountTable = new HashMap<String, MountInfo>(initialCapacity);
  }

  /**
   * Mounts the given UFS path at the given Alluxio path. The Alluxio path should not be nested
   * under an existing mount point.
   *
   * @param alluxioUri an Alluxio path URI
   * @param ufsUri a UFS path URI
   * @param options the mount options
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized void add(AlluxioURI alluxioUri, AlluxioURI ufsUri, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException {
    String alluxioPath = alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, alluxioPath);
    if (mMountTable.containsKey(alluxioPath)) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(alluxioPath));
    }
    // Check all non-root mount points, to check if they're a prefix of the alluxioPath we're trying
    // to mount. Also make sure that the ufs path we're trying to mount is not a prefix or suffix of
    // any existing mount path.
    for (Map.Entry<String, MountInfo> entry : mMountTable.entrySet()) {
      String mountedAlluxioPath = entry.getKey();
      AlluxioURI mountedUfsUri = entry.getValue().getUfsUri();
      if (!mountedAlluxioPath.equals(ROOT)
          && PathUtils.hasPrefix(alluxioPath, mountedAlluxioPath)) {
        throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
            mountedAlluxioPath, alluxioPath));
      } else if ((ufsUri.getScheme() == null || ufsUri.getScheme()
          .equals(mountedUfsUri.getScheme()))
          && (ufsUri.getAuthority() == null || ufsUri.getAuthority().equals(
              mountedUfsUri.getAuthority()))) {
        String ufsPath = ufsUri.getPath();
        String mountedUfsPath = mountedUfsUri.getPath();
        if (PathUtils.hasPrefix(ufsPath, mountedUfsPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
              mountedUfsUri.toString(), ufsUri.toString()));
        } else if (PathUtils.hasPrefix(mountedUfsPath, ufsPath)) {
          throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
              ufsUri.toString(), mountedUfsUri.toString()));
        }
      }
    }
    mMountTable.put(alluxioPath, new MountInfo(ufsUri, options));
  }

  /**
   * Unmounts the given Alluxio path. The path should match an existing mount point.
   *
   * @param uri an Alluxio path URI
   * @return whether the operation succeeded or not
   */
  public synchronized boolean delete(AlluxioURI uri) {
    String path = uri.getPath();
    LOG.info("Unmounting {}", path);
    if (path.equals(ROOT)) {
      LOG.warn("Cannot unmount the root mount point.");
      return false;
    }
    if (mMountTable.containsKey(path)) {
      mMountTable.remove(path);
      return true;
    }
    LOG.warn("Mount point {} does not exist.", path);
    return false;
  }

  /**
   * Returns the closest ancestor mount point the given path is nested under.
   *
   * @param uri an Alluxio path URI
   * @return mount point the given Alluxio path is nested under
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized String getMountPoint(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    String mountPoint = null;
    for (Map.Entry<String, MountInfo> entry : mMountTable.entrySet()) {
      String alluxioPath = entry.getKey();
      if (PathUtils.hasPrefix(path, alluxioPath)
          && (mountPoint == null || PathUtils.hasPrefix(alluxioPath, mountPoint))) {
        mountPoint = alluxioPath;
      }
    }
    return mountPoint;
  }

  /**
   * @param uri an Alluxio path URI
   * @return whether the given uri is a mount point
   */
  public synchronized boolean isMountPoint(AlluxioURI uri) {
    return mMountTable.containsKey(uri.getPath());
  }

  /**
   * Resolves the given Alluxio path. If the given Alluxio path is nested under a mount point, the
   * resolution maps the Alluxio path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param uri an Alluxio path URI
   * @return the {@link Resolution} respresenting the UFS path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized Resolution resolve(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    LOG.debug("Resolving {}", path);
    String mountPoint = getMountPoint(uri);
    if (mountPoint != null) {
      MountInfo info = mMountTable.get(mountPoint);
      AlluxioURI ufsUri = info.getUfsUri();
      // TODO(gpang): this ufs should probably be cached.
      UnderFileSystem ufs = UnderFileSystem.get(ufsUri.toString(), MasterContext.getConf());
      ufs.setProperties(info.getOptions().getProperties());
      AlluxioURI resolvedUri = ufs.resolveUri(ufsUri, path.substring(mountPoint.length()));
      return new Resolution(resolvedUri, ufs);
    }
    return new Resolution(uri, null);
  }

  /**
   * Checks to see if a write operation is allowed for the specified Alluxio path, by determining
   * if it is under a readonly mount point.
   *
   * @param alluxioUri an Alluxio path URI
   * @throws InvalidPathException if the Alluxio path is invalid
   * @throws AccessControlException if the Alluxio path is under a readonly mount point
   */
  public synchronized void checkUnderWritableMountPoint(AlluxioURI alluxioUri)
      throws InvalidPathException, AccessControlException {
    String mountPoint = getMountPoint(alluxioUri);
    MountInfo mountInfo = mMountTable.get(mountPoint);
    if (mountInfo.getOptions().isReadOnly()) {
      throw new AccessControlException(ExceptionMessage.MOUNT_READONLY, alluxioUri, mountPoint);
    }
  }

  /**
   * This class represents a UFS path after resolution. The UFS URI and the {@link UnderFileSystem}
   * for the UFS path are available.
   */
  public final class Resolution {
    private final AlluxioURI mUri;
    private final UnderFileSystem mUfs;

    private Resolution(AlluxioURI uri, UnderFileSystem ufs) {
      mUri = uri;
      mUfs = ufs;
    }

    /**
     * @return the URI in the ufs
     */
    public AlluxioURI getUri() {
      return mUri;
    }

    /**
     * @return the {@link UnderFileSystem} instance
     */
    public UnderFileSystem getUfs() {
      return mUfs;
    }
  }
}

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
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.MountOptions;
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
   * Returns the mount point the given path is nested under.
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
   * Returns in indication of whether the given path is a mount point.
   *
   * @param uri an Alluxio path URI
   * @return mount point the given Alluxio path is nested under
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
   * @return the resolved path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized AlluxioURI resolve(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    LOG.debug("Resolving {}", path);
    String mountPoint = getMountPoint(uri);
    if (mountPoint != null) {
      AlluxioURI ufsPath = mMountTable.get(mountPoint).getUfsUri();
      return new AlluxioURI(ufsPath.getScheme(), ufsPath.getAuthority(),
          PathUtils.concatPath(ufsPath.getPath(), path.substring(mountPoint.length())));
    }
    return uri;
  }

  /**
   * Checks to see if a write operation is allowed for the specified Alluxio path, by determining
   * if it is under a readonly mount point.
   *
   * @param alluxioUri an Alluxio path URI
   * @throws InvalidPathException if the Alluxio path is invalid
   * @throws AccessControlException if the Alluxio path is under a readonly mount point
   */
  public synchronized void checkWritable(AlluxioURI alluxioUri)
      throws InvalidPathException, AccessControlException {
    String mountPoint = getMountPoint(alluxioUri);
    MountInfo mountInfo = mMountTable.get(mountPoint);
    if (mountInfo.getOptions().isReadOnly()) {
      throw new AccessControlException(ExceptionMessage.MOUNT_READONLY, alluxioUri, mountPoint);
    }
  }
}

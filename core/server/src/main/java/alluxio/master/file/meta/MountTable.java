/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.file.meta;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;

/**
 * This class is used for keeping track of Tachyon mount points.
 */
@ThreadSafe
public final class MountTable {
  public static final String ROOT = "/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<String, AlluxioURI> mMountTable;

  /**
   * Creates a new instance of {@link MountTable}.
   */
  public MountTable() {
    final int initialCapacity = 10;
    mMountTable = new HashMap<String, AlluxioURI>(initialCapacity);
  }

  /**
   * Mounts the given UFS path at the given Tachyon path. The Tachyon path should not be nested
   * under an existing mount point.
   *
   * @param alluxioUri a Tachyon path URI
   * @param ufsUri a UFS path URI
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized void add(AlluxioURI alluxioUri, AlluxioURI ufsUri)
      throws FileAlreadyExistsException, InvalidPathException {
    String tachyonPath = alluxioUri.getPath();
    LOG.info("Mounting {} at {}", ufsUri, tachyonPath);
    if (mMountTable.containsKey(tachyonPath)) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(tachyonPath));
    }
    // Check all non-root mount points, to check if they're a prefix of the tachyonPath we're trying
    // to mount. Also make sure that the ufs path we're trying to mount is not a prefix or suffix of
    // any existing mount path.
    for (Map.Entry<String, AlluxioURI> entry : mMountTable.entrySet()) {
      String mountedTachyonPath = entry.getKey();
      AlluxioURI mountedUfsUri = entry.getValue();
      if (!mountedTachyonPath.equals(ROOT)
          && PathUtils.hasPrefix(tachyonPath, mountedTachyonPath)) {
        throw new InvalidPathException(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage(
            mountedTachyonPath, tachyonPath));
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
    mMountTable.put(tachyonPath, ufsUri);
  }

  /**
   * Unmounts the given Tachyon path. The path should match an existing mount point.
   *
   * @param uri a Tachyon path URI
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
   * @param uri a Tachyon path URI
   * @return mount point the given Tachyon path is nested under
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized String getMountPoint(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    String mountPoint = null;
    for (Map.Entry<String, AlluxioURI> entry : mMountTable.entrySet()) {
      String tachyonPath = entry.getKey();
      if (PathUtils.hasPrefix(path, tachyonPath)
          && (mountPoint == null || PathUtils.hasPrefix(tachyonPath, mountPoint))) {
        mountPoint = tachyonPath;
      }
    }
    return mountPoint;
  }

  /**
   * Returns in indication of whether the given path is a mount point.
   *
   * @param uri a Tachyon path URI
   * @return mount point the given Tachyon path is nested under
   */
  public synchronized boolean isMountPoint(AlluxioURI uri) {
    return mMountTable.containsKey(uri.getPath());
  }

  /**
   * Resolves the given Tachyon path. If the given Tachyon path is nested under a mount point, the
   * resolution maps the Tachyon path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param uri a Tachyon path URI
   * @return the resolved path
   * @throws InvalidPathException if an invalid path is encountered
   */
  public synchronized AlluxioURI resolve(AlluxioURI uri) throws InvalidPathException {
    String path = uri.getPath();
    LOG.debug("Resolving {}", path);
    String mountPoint = getMountPoint(uri);
    if (mountPoint != null) {
      AlluxioURI ufsPath = mMountTable.get(mountPoint);
      return new AlluxioURI(ufsPath.getScheme(), ufsPath.getAuthority(),
          PathUtils.concatPath(ufsPath.getPath(), path.substring(mountPoint.length())));
    }
    return uri;
  }
}

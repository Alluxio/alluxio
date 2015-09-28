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

package tachyon.master.file.meta;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.thrift.InvalidPathException;
import tachyon.util.io.PathUtils;

/** This class is used for keeping track of Tachyon mount points. It is thread safe. */
public final class MountTable {
  public static final String ROOT = "/";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<String, TachyonURI> mMountTable;

  /**
   * Creates a new instance of <code>MountTable</code>.
   */
  public MountTable() {
    final int initialCapacity = 10;
    mMountTable = new HashMap<String, TachyonURI>(initialCapacity);
  }

  /**
   * Mounts the given UFS path at the given Tachyon path. The Tachyon path should not be nested
   * under an existing mount point.
   *
   * @param tachyonUri a Tachyon path URI
   * @param ufsUri a UFS path URI
   * @return whether the operation succeeded or not
   */
  public synchronized boolean add(TachyonURI tachyonUri, TachyonURI ufsUri)
      throws InvalidPathException {
    String tachyonPath = tachyonUri.getPath();
    LOG.info("Mounting " + ufsUri + " at " + tachyonPath);
    for (Map.Entry<String, TachyonURI> entry : mMountTable.entrySet()) {
      String path = entry.getKey();
      if (tachyonPath.equals(path)) {
        LOG.warn("Mount point " + tachyonPath + " already exists.");
        return false;
      }
      if (!path.equals(ROOT) && PathUtils.hasPrefix(tachyonPath, path)) {
        LOG.warn("Cannot nest mount point " + tachyonPath + " under existing mount point" + path
            + ".");
        return false;
      }
    }
    mMountTable.put(tachyonPath, ufsUri);
    return true;
  }

  /**
   * Unmounts the given Tachyon path. The path should match an existing mount point.
   *
   * @param uri a Tachyon path URI
   * @return whether the operation succeeded or not
   */
  public synchronized boolean delete(TachyonURI uri) throws InvalidPathException {
    String path = uri.getPath();
    LOG.info("Unmounting " + path);
    if (path.equals(ROOT)) {
      LOG.warn("Cannot unmount the root mount point.");
      return false;
    }
    if (mMountTable.containsKey(path)) {
      mMountTable.remove(path);
      return true;
    }
    LOG.warn("Mount point " + path + " does not exist.");
    return false;
  }

  /**
   * Returns the mount point the given path is nested under.
   *
   * @param uri a Tachyon path URI
   * @return mount point the given Tachyon path is nested under
   */
  public synchronized String getMountPoint(TachyonURI uri) throws InvalidPathException {
    String path = uri.getPath();
    String mountPoint = null;
    for (Map.Entry<String, TachyonURI> entry : mMountTable.entrySet()) {
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
  public synchronized boolean isMountPoint(TachyonURI uri) throws InvalidPathException {
    return mMountTable.containsKey(uri.getPath());
  }

  /**
   * Resolves the given Tachyon path. If the given Tachyon path is nested under a mount point, the
   * resolution maps the Tachyon path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param uri a Tachyon path URI
   * @return the resolved path
   */
  public synchronized TachyonURI resolve(TachyonURI uri) throws InvalidPathException {
    String path = uri.getPath();
    LOG.info("Resolving " + path);
    String mountPoint = getMountPoint(uri);
    if (mountPoint != null) {
      TachyonURI ufsPath = mMountTable.get(mountPoint);
      return new TachyonURI(ufsPath.getScheme(), ufsPath.getAuthority(), PathUtils.concatPath(
          ufsPath.getPath(), path.substring(mountPoint.length())));
    }
    return uri;
  }
}

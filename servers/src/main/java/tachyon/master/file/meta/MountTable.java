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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<String, TachyonURI> mMountTable;

  /**
   * Creates a new instance of <code>MountTable</code>.
   */
  public MountTable() {
    final int INITIAL_CAPACITY = 10;
    mMountTable = new HashMap<String, TachyonURI>(INITIAL_CAPACITY);
  }

  /**
   * Mounts the given UFS path at the given Tachyon path. The Tachyon path should not be nested
   * under an existing mount point.
   *
   * @param tachyonPath a Tachyon path
   * @param ufsPath a URI identifying the UFS path
   * @return whether the operation succeeded or not
   */
  public synchronized boolean add(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws InvalidPathException {
    LOG.info("Mounting " + ufsPath + " at " + tachyonPath.getPath());
    for (Map.Entry<String, TachyonURI> entry : mMountTable.entrySet()) {
      String path = entry.getKey();
      if (PathUtils.hasPrefix(tachyonPath.getPath(), path)
          || PathUtils.hasPrefix(path, tachyonPath.getPath())) {
        // Cannot mount a path under an existing mount point.
        return false;
      }
    }
    mMountTable.put(tachyonPath.getPath(), ufsPath);
    return true;
  }

  /**
   * Unmounts the given Tachyon path. The path should match an existing mount point.
   *
   * @param path a Tachyon path
   * @return whether the operation succeeded or not
   */
  public synchronized boolean delete(TachyonURI path) {
    LOG.info("Unmounting " + path.getPath());
    if (mMountTable.containsKey(path.getPath())) {
      mMountTable.remove(path.getPath());
      return true;
    }
    // Cannot unmount a path that does not correspond to a mount point.
    return false;
  }

  /**
   * Returns the mount point the given path is nested under.
   *
   * @param path a Tachyon path
   * @return mount point the given Tachyon path is nested under
   */
  public synchronized String getMountPoint(TachyonURI path) throws InvalidPathException {
    for (Map.Entry<String, TachyonURI> entry : mMountTable.entrySet()) {
      String tachyonPath = entry.getKey();
      if (PathUtils.hasPrefix(path.getPath(), tachyonPath)) {
        return tachyonPath;
      }
    }
    return null;
  }

  /**
   * Resolves the given Tachyon path. If the given Tachyon path is nested under a mount point, the
   * resolution maps the Tachyon path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param path a Tachyon path
   * @return the resolved path
   */
  public synchronized TachyonURI resolve(TachyonURI path) throws InvalidPathException {
    LOG.info("Resolving " + path);
    for (Map.Entry<String, TachyonURI> entry : mMountTable.entrySet()) {
      String tachyonPath = entry.getKey();
      TachyonURI ufsPath = entry.getValue();
      if (PathUtils.hasPrefix(path.getPath(), tachyonPath)) {
        return new TachyonURI(ufsPath.getScheme(), ufsPath.getAuthority(), ufsPath.getPath()
            + path.getPath().substring(tachyonPath.length()));
      }
    }
    // If the given path is not found in the mount table, return the original URI.
    return path;
  }
}

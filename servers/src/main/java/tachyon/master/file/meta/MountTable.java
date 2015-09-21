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
public class MountTable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<TachyonURI, TachyonURI> mMountTable;

  /**
   * Creates a new instance of <code>MountTable</code>.
   */
  public MountTable() {
    final int INITIAL_CAPACITY = 10;
    mMountTable = new HashMap<TachyonURI, TachyonURI>(INITIAL_CAPACITY);
  }

  /**
   * Mounts the given UFS path at the given Tachyon path. The Tachyon path should not be nested
   * under an existing mount point.
   *
   * @param tachyonPath a Tachyon path
   * @param ufsPath a UFS path
   * @return whether the operation succeeded or not
   */
  public synchronized boolean add(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws InvalidPathException {
    LOG.info("Mounting " + ufsPath + " under " + tachyonPath);
    for (Map.Entry<TachyonURI, TachyonURI> entry : mMountTable.entrySet()) {
      TachyonURI path = entry.getKey();
      if (hasPrefix(tachyonPath, path) || hasPrefix(path, tachyonPath)) {
        // Cannot mount a path under an existing mount point.
        return false;
      }
    }
    mMountTable.put(tachyonPath, ufsPath);
    return true;
  }

  /**
   * Unmounts the given Tachyon path. The path should match an existing mount point.
   *
   * @param tachyonPath a Tachyon path
   * @return whether the operation succeeded or not
   */
  public synchronized boolean delete(TachyonURI tachyonPath) {
    LOG.info("Unmounting " + tachyonPath);
    if (mMountTable.containsKey(tachyonPath)) {
      mMountTable.remove(tachyonPath);
      return true;
    }
    // Cannot unmount a path that does not correspond to a mount point.
    return false;
  }

  /**
   * Returns the mount point the given path is nested under.
   *
   * @param tachyonPath a Tachyon path
   * @return mount point the given Tachyon path is nested under
   */
  public synchronized TachyonURI getMountPoint(TachyonURI tachyonPath) throws InvalidPathException {
    for (Map.Entry<TachyonURI, TachyonURI> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        return entry.getKey();
      }
    }
    return new TachyonURI("");
  }

  /**
   * Resolves the given Tachyon path. If the given Tachyon path is nested under a mount point, the
   * resolution maps the Tachyon path to the corresponding UFS path. Otherwise, the resolution is a
   * no-op.
   *
   * @param tachyonPath a Tachyon path
   * @return the resolved path
   */
  public synchronized TachyonURI resolve(TachyonURI tachyonPath) throws InvalidPathException {
    LOG.info("Resolving " + tachyonPath);
    for (Map.Entry<TachyonURI, TachyonURI> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        return new TachyonURI(entry.getValue()
            + tachyonPath.toString().substring(entry.getKey().toString().length()));
      }
    }
    // If the given path is not found in the mount table, return the original URI.
    return tachyonPath;
  }

  /**
   * @param path a path
   * @param prefix a prefix
   * @return whether the given path has the given prefix
   */
  private boolean hasPrefix(TachyonURI path, TachyonURI prefix) throws InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.toString());
    String[] prefixComponents = PathUtils.getPathComponents(prefix.toString());
    if (pathComponents.length < prefixComponents.length) {
      return false;
    }
    for (int i = 0; i < prefixComponents.length; i ++) {
      if (!pathComponents[i].equals(prefixComponents[i])) {
        return false;
      }
    }
    return true;
  }
}

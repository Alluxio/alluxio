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

/** This class is used for keeping track of Tachyon mount points. It is thread safe. */
public class MountTable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<TachyonURI, TachyonURI> mMountTable;

  public MountTable() {
    final int INITIAL_CAPACITY = 10;
    mMountTable = new HashMap<TachyonURI, TachyonURI>(INITIAL_CAPACITY);
  }

  public synchronized boolean add(TachyonURI tachyonPath, TachyonURI ufsPath) {
    LOG.debug("Mounting " + ufsPath + " under " + tachyonPath);
    for (Map.Entry<TachyonURI, TachyonURI> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        // Cannot mount a path under an existing mount point.
        return false;
      }
    }
    mMountTable.put(tachyonPath, ufsPath);
    return true;
  }

  public synchronized boolean delete(TachyonURI tachyonPath) {
    LOG.debug("Unmounting " + tachyonPath);
    if (mMountTable.containsKey(tachyonPath)) {
      mMountTable.remove(tachyonPath);
      return true;
    }
    // Cannot mount a path under an existing mount point.
    return false;
  }

  public synchronized TachyonURI lookup(TachyonURI tachyonPath) {
    LOG.debug("Looking up " + tachyonPath);
    for (Map.Entry<TachyonURI, TachyonURI> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        return new TachyonURI(entry.getValue()
            + tachyonPath.toString().substring(entry.getKey().toString().length()));
      }
    }
    // If the given path is not found in the mount table, the lookup is an identity.
    return tachyonPath;
  }

  private boolean hasPrefix(TachyonURI path, TachyonURI prefix) {
    return path.toString().startsWith(prefix.toString());
  }
}

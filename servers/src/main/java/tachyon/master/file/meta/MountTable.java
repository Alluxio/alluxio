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

import tachyon.exception.AlreadyExistsException;
import tachyon.exception.NotFoundException;

/** This class is used for keeping track of Tachyon mount points. It is thread safe. */
public class MountTable {
  private Map<String, String> mMountTable;

  public MountTable() {
    final int INITIAL_CAPACITY = 10;
    mMountTable = new HashMap<String, String>(INITIAL_CAPACITY);
  }

  public synchronized void add(String tachyonPath, String ufsPath) throws AlreadyExistsException {
    for (Map.Entry<String, String> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        // Cannot mount a path under an existing mount point.
        throw new AlreadyExistsException("Tachyon path " + tachyonPath
            + " cannot be mounted under an existing mount point " + entry.getValue());
      }
    }
    mMountTable.put(tachyonPath, ufsPath);
  }

  public synchronized void delete(String tachyonPath) throws NotFoundException {
    if (mMountTable.containsKey(tachyonPath)) {
      mMountTable.remove(tachyonPath);
    }
    // Cannot mount a path under an existing mount point.
    throw new NotFoundException("Tachyon path " + tachyonPath + " is not a valid mount point.");
  }

  public synchronized String lookup(String tachyonPath) {
    for (Map.Entry<String, String> entry : mMountTable.entrySet()) {
      if (hasPrefix(tachyonPath, entry.getKey())) {
        return entry.getValue() + tachyonPath.substring(entry.getKey().length());
      }
    }
    // If the given path is not found in the mount table, the lookup is an identity.
    return tachyonPath;
  }

  public synchronized String reverseLookup(String ufsPath) throws NotFoundException {
    for (Map.Entry<String, String> entry : mMountTable.entrySet()) {
      if (hasPrefix(ufsPath, entry.getValue())) {
        return entry.getKey() + ufsPath.substring(entry.getValue().length());
      }
    }
    // If the given path is not found in the mount table, the reverse lookup fails.
    throw new NotFoundException("UFS path " + ufsPath + " is not mounted in Tachyon namespace.");
  }

  private boolean hasPrefix(String path, String prefix) {
    // TODO(jiri): Use canonical representation for UFS scheme and authority.
    return path.startsWith(prefix);
  }
}

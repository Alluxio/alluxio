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

import alluxio.exception.InvalidPathException;
import alluxio.util.UnderFileSystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class that manages the UFS for master servers.
 */
@ThreadSafe
public final class MasterUfsManager extends AbstractUfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(MasterUfsManager.class);

  // The physical ufs state for all active mounts if not the default
  private ConcurrentHashMap<String, UnderFileSystem.UfsMode> mPhysicalUfsState =
      new ConcurrentHashMap<>();

  /**
   * Constructs the instance of {@link MasterUfsManager}.
   */
  public MasterUfsManager() {}

  @Override
  public void removeMount(long mountId) {
    super.removeMount(mountId);

    // Remove any unused physical paths from map
    for (String physicalUfs : mPhysicalUfsState.keySet()) {
      boolean found = false;
      for (UnderFileSystem ufs : mUnderFileSystemMap.values()) {
        if (ufs.getPhysicalUfs().contains(physicalUfs)) {
          found = true;
          break;
        }
      }
      if (!found) {
        mPhysicalUfsState.remove(physicalUfs);
        return;
      }
    }
  }

  /**
   * @return the state of physical UFSs in maintenance
   */
  public Map<String, UnderFileSystem.UfsMode> getPhysicalUfsState() {
    return mPhysicalUfsState;
  }

  /**
   * Set the operation mode the given physical ufs.
   *
   * @param ufsPath the physical ufs path (scheme and authority only)
   * @param ufsMode the ufs operation mode
   * @throws InvalidPathException if no managed ufs covers the given path
   */
  public void setUfsMode(String ufsPath, UnderFileSystem.UfsMode ufsMode)
      throws InvalidPathException {
    LOG.info("Set ufs mode for {} to {}", ufsPath, ufsMode);
    for (UnderFileSystem ufs : mUnderFileSystemMap.values()) {
      String key = UnderFileSystemUtils.stripFolderFromPath(ufsPath);
      if (ufs.getPhysicalUfs().contains(key)) {
        // Found a managed ufs for the given physical path
        if (ufsMode == UnderFileSystem.UfsMode.READ_WRITE) {
          // Remove key from map if exists
          mPhysicalUfsState.remove(key);
        } else {
          // Set the maintenance state of the given ufs
          mPhysicalUfsState.put(key, ufsMode);
        }
        return;
      }
    }
    // No managed ufs uses the given physical ufs path
    LOG.warn("No managed ufs for physical ufs path {}", ufsPath);
    throw new InvalidPathException(String.format("Ufs path %s is not managed", ufsPath));
  }
}

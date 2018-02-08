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

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.InvalidPathException;
import alluxio.resource.CloseableResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class that manages the UFS for master servers.
 */
@ThreadSafe
public final class MasterUfsManager extends AbstractUfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(MasterUfsManager.class);

  /**
   * {@link alluxio.underfs.UnderFileSystem.UfsMode} and mount ids corresponding to a physical ufs.
   */
  public static class UfsState {
    private UnderFileSystem.UfsMode mUfsMode;
    private ConcurrentHashSet<Long> mMountIds;

    /**
     * Construct a new instance of UfsState w/ defaults.
     */
    UfsState() {
      mUfsMode = UnderFileSystem.UfsMode.READ_WRITE;
      mMountIds = new ConcurrentHashSet<>();
    }

    /**
     * Add a mount id to list using physical ufs.
     *
     * @param mountId the mount id
     */
    void addMount(long mountId) {
      mMountIds.addIfAbsent(mountId);
    }

    /**
     * Remove mount id from list using physical ufs.
     *
     * @param mountId the mount id
     * @return true, if mount list is empty
     */
    boolean removeMount(long mountId) {
      mMountIds.remove(mountId);
      return mMountIds.size() == 0;
    }

    /**
     * @return the physical ufs operation mode
     */
    UnderFileSystem.UfsMode getUfsMode() {
      return mUfsMode;
    }

    /**
     * Set the operation mode.
     *
     * @param ufsMode the ufs operation mode
     */
    void setUfsMode(UnderFileSystem.UfsMode ufsMode) {
      mUfsMode = ufsMode;
    }
  }

  // The physical ufs state for all managed mounts
  private ConcurrentHashMap<String, UfsState> mPhysicalUfsToState =
      new ConcurrentHashMap<>();

  /**
   * Constructs the instance of {@link MasterUfsManager}.
   */
  public MasterUfsManager() {}

  @Override
  public void addMount(long mountId, final AlluxioURI ufsUri,
                       final UnderFileSystemConfiguration ufsConf) {
    super.addMount(mountId, ufsUri, ufsConf);

    try (CloseableResource<UnderFileSystem> ufsResource = get(mountId).acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      for (String physicalUfs : ufs.getPhysicalStores()) {
        mPhysicalUfsToState.compute(physicalUfs, (k, v) -> {
          if (v == null) {
            v = new UfsState();
          }
          v.addMount(mountId);
          return v;
        });
      }
    } catch (Exception e) {
      // Unable to resolve mount point
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void removeMount(long mountId) {
    try (CloseableResource<UnderFileSystem> ufsResource = get(mountId).acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      for (String physicalUfs : ufs.getPhysicalStores()) {
        mPhysicalUfsToState.computeIfPresent(physicalUfs, (k, v) -> {
          if (v.removeMount(mountId)) {
            // Remove key if list is empty
            return null;
          }
          return v;
        });
      }
    } catch (Exception e) {
      // Unable to resolve mount point
      LOG.error(e.getMessage());
    }

    super.removeMount(mountId);
  }

  /**
   * Get the physical ufs operation modes for the {@link UnderFileSystem} under the given Mount
   * table resolution.
   *
   * @param physicalStores the physical stores for the mount resolution
   * @return the state of physical UFS for given mount resolution
   */
  public Map<String, UnderFileSystem.UfsMode> getPhysicalUfsState(List<String> physicalStores) {
    Map<String, UnderFileSystem.UfsMode> ufsModeState = new HashMap<>();
    for (String physicalUfs : physicalStores) {
      UfsState ufsState = mPhysicalUfsToState.get(new AlluxioURI(physicalUfs).getRootPath());
      if (ufsState != null) {
        ufsModeState.put(physicalUfs, ufsState.getUfsMode());
      }
    }
    return ufsModeState;
  }

  /**
   * Set the operation mode the given physical ufs.
   *
   * @param ufsPath the physical ufs path (scheme and authority only)
   * @param ufsMode the ufs operation mode
   * @throws InvalidPathException if no managed ufs covers the given path
   */
  public void setUfsMode(AlluxioURI ufsPath, UnderFileSystem.UfsMode ufsMode)
      throws InvalidPathException {
    LOG.info("Set ufs mode for {} to {}", ufsPath, ufsMode);
    String key = ufsPath.getRootPath();
    UfsState state = mPhysicalUfsToState.compute(key, (k, v) -> {
      if (v == null) {
        // No managed ufs uses the given physical ufs path
        return v;
      }
      // Found a managed ufs for the given physical path
      v.setUfsMode(ufsMode);
      return v;
    });

    if (state == null) {
      LOG.warn("No managed ufs for physical ufs path {}", ufsPath);
      throw new InvalidPathException(String.format("Ufs path %s is not managed", ufsPath));
    }
  }
}

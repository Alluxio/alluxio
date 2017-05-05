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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class UfsAbsentPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(UfsAbsentPathCache.class);

  /** The mount table. */
  private final MountTable mMountTable;
  /** Stores a cache for each mount point (the key is mount id). */
  private final ConcurrentHashMapV8<Long, ConcurrentSkipListSet<String>> mCaches;

  /**
   * Creates a new instance of {@link UfsAbsentPathCache}.
   *
   * @param mountTable the mount table
   */
  public UfsAbsentPathCache(MountTable mountTable) {
    mMountTable = mountTable;
    mCaches = new ConcurrentHashMapV8<>(8, 0.95f, 8);
  }

  public void removeMountPoint(long mountId) {
    mCaches.remove(mountId);
  }

  public void addAbsentPath(AlluxioURI path) throws InvalidPathException, IOException {
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();
    ConcurrentSkipListSet<String> cache = getCache(resolution.getMountId());

    String[] components = PathUtils.getPathComponents(ufsUri.getPath());
    if (components.length == 0) {
      return;
    }

    // create a ufs uri of the root.
    AlluxioURI uri =
        new AlluxioURI(ufsUri.getScheme(), ufsUri.getAuthority(), "/", ufsUri.getQueryMap());

    // Traverse through the ufs path components, staring from the root, to find the first
    // non-existing ufs path.
    for (String component : components) {
      uri = uri.join(component);
      String uriPath = uri.getPath();
      if (ufs.exists(uri.toString())) {
        // This ufs path exists. Remove the cache entry.
        cache.remove(uriPath);
      } else {
        // This is the first ufs path which does not exist. Add it to the cache.
        cache.add(uriPath);

        // Remove cache entries which has this non-existing directory as a prefix. This is not for
        // correctness, but to "compress" information.
        String dirPath = uriPath + "/";
        Iterator<String> it = cache.tailSet(dirPath).iterator();
        while (it.hasNext()) {
          String existingPath = it.next();
          if (existingPath.startsWith(dirPath)) {
            // An existing cache entry has the non-existing path as a prefix. Remove the entry,
            // since the non-existing path ancestor implies the descendant does not exist.
            it.remove();
          }
        }
        // The first non-existing path was found, so further traversal is unnecessary.
        break;
      }
    }
  }

  public void removeAbsentPath(AlluxioURI path) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    ConcurrentSkipListSet<String> cache = getCache(resolution.getMountId());

    String[] components = PathUtils.getPathComponents(ufsUri.getPath());

    // create a ufs uri of the root.
    AlluxioURI uri =
        new AlluxioURI(ufsUri.getScheme(), ufsUri.getAuthority(), "/", ufsUri.getQueryMap());

    // Traverse through the ufs path components, staring from the root.
    for (String component : components) {
      uri = uri.join(component);
      String uriPath = uri.getPath();
      // This ufs path exists. Remove the cache entry.
      cache.remove(uriPath);
    }
  }

  public boolean isAbsent(AlluxioURI path) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    ConcurrentSkipListSet<String> cache = getCache(resolution.getMountId());

    while (ufsUri != null) {
      if (cache.contains(ufsUri.getPath())) {
        return true;
      }
      ufsUri = ufsUri.getParent();
    }
    // Reached the root, without finding anything in the cache.
    return false;
  }

  private ConcurrentSkipListSet<String> getCache(long mountId) {
    ConcurrentSkipListSet<String> set = mCaches.get(mountId);
    if (set != null) {
      return set;
    }

    set = new ConcurrentSkipListSet<>();
    ConcurrentSkipListSet<String> existing = mCaches.putIfAbsent(mountId, set);
    if (existing != null) {
      return existing;
    }
    return set;
  }
}

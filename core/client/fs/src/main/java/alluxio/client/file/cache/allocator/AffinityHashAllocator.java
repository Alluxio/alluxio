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

package alluxio.client.file.cache.allocator;

import alluxio.client.file.cache.store.PageStoreDir;

import java.util.List;
import java.util.function.Function;

/**
 * Offers stickiness allocation based on the file id.
 */
public class AffinityHashAllocator extends HashAllocator {

  /**
   * @param dirs page store directories
   */
  public AffinityHashAllocator(List<PageStoreDir> dirs) {
    super(dirs);
  }

  /**
   * @param dirs page store directories
   * @param hashFunction hash function
   */
  public AffinityHashAllocator(List<PageStoreDir> dirs, Function<String, Integer> hashFunction) {
    super(dirs, hashFunction);
  }

  @Override
  public PageStoreDir allocate(String fileId, long fileLength) {
    for (PageStoreDir dir : mDirs) {
      if (dir.hasFile(fileId) || dir.hasTempFile(fileId)) {
        return dir;
      }
    }
    return super.allocate(fileId, fileLength);
  }
}

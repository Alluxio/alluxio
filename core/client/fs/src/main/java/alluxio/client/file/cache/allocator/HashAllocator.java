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

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.util.Objects.requireNonNull;

import alluxio.client.file.cache.store.PageStoreDir;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

/**
 * An implementation of Allocator base on the hash of the file id for local cache.
 */
public class HashAllocator implements Allocator {

  protected final List<PageStoreDir> mDirs;
  private final Function<String, Integer> mHashFunction;

  /**
   * @param dirs page store directories
   */
  public HashAllocator(List<PageStoreDir> dirs) {
    this(dirs, (fileId) -> murmur3_32_fixed().hashString(fileId, StandardCharsets.UTF_8).asInt());
  }

  /**
   * @param dirs page store directories
   * @param hashFunction hash function
   */
  public HashAllocator(List<PageStoreDir> dirs, Function<String, Integer> hashFunction) {
    mDirs = requireNonNull(dirs);
    mHashFunction = requireNonNull(hashFunction);
  }

  @Override
  public PageStoreDir allocate(String fileId, long fileLength) {
    return mDirs.get(
        Math.floorMod(mHashFunction.apply(fileId),
            mDirs.size()));
  }
}

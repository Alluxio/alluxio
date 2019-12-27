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

package alluxio.client.file;

import java.io.IOException;

/**
 * A wrapper class that will remove itself from the cache on close.
 */
public class InstanceCachingFileSystem extends DelegatingFileSystem {
  private final FileSystemCache mCache;
  private final FileSystemCache.Key mKey;

  /**
   * Wraps a file system instance to cache.
   *
   * @param fs file system context
   * @param cache fs instance cache
   * @param key key in fs instance cache
   */
  InstanceCachingFileSystem(FileSystem fs, FileSystemCache cache, FileSystemCache.Key key) {
    super(fs);
    mCache = cache;
    mKey = key;
  }

  @Override
  public void close() throws IOException {
    super.close();
    // TODO(binfan): is this the expected behavior? shouldn't we keep refcount in cache?
    mCache.remove(mKey);
  }
}

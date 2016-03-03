/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * “License”). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.orangefs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link OrangeFSUnderFileSystem}.
 *
 * It caches created {@link OrangeFSUnderFileSystem}s, using the scheme and authority pair as the
 * key.
 */
@ThreadSafe
public final class OrangeFSUnderFileSystemFactory implements UnderFileSystemFactory {
  /**
   * Cache mapping {@code Path}s to existing {@link UnderFileSystem} instances. The paths should be
   * normalized to root paths because only their schemes and authorities are needed to identify
   * which OrangeFS file system they belong to.
   */
  private Map<AlluxioURI, OrangeFSUnderFileSystem> mOrangeFSUfsCache = Maps.newHashMap();

  @Override
  public UnderFileSystem create(String path, Configuration configuration, Object unusedConf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    // Normalize the path to just its root. This is all that's needed to identify which FileSystem
    // the Path belongs to.
    AlluxioURI rootPath = getRoot(new AlluxioURI(path));
    synchronized (mOrangeFSUfsCache) {
      if (!mOrangeFSUfsCache.containsKey(rootPath)) {
        try {
          mOrangeFSUfsCache.put(rootPath, new OrangeFSUnderFileSystem(path, configuration));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return mOrangeFSUfsCache.get(rootPath);
    }
  }

  private static AlluxioURI getRoot(AlluxioURI path) {
    AlluxioURI currPath = path;
    while (currPath.getParent() != null) {
      currPath = currPath.getParent();
    }
    return currPath;
  }

  @Override
  public boolean supportsPath(String path, Configuration conf) {
    return path != null && path.startsWith(Constants.HEADER_OFS);
  }
}

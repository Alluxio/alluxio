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

package tachyon.underfs.hdfs;

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;

/**
 * Factory for creating {@link HdfsUnderFileSystem}.
 *
 * It caches created {@link HdfsUnderFileSystem}s, using the scheme and authority pair as the key.
 */
public final class HdfsUnderFileSystemFactory implements UnderFileSystemFactory {
  /**
   * Cache mapping {@code Path}s to existing {@link UnderFileSystem} instances. The paths should be
   * normalized to root paths because only their schemes and authorities are needed to identify
   * which {@link FileSystem} they belong to.
   */
  private Map<Path, HdfsUnderFileSystem> mHdfsUfsCache = Maps.newHashMap();

  @Override
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    // Normalize the path to just its root. This is all that's needed to identify which FileSystem
    // the Path belongs to.
    Path rootPath = getRoot(new Path(path));
    synchronized (mHdfsUfsCache) {
      if (!mHdfsUfsCache.containsKey(rootPath)) {
        mHdfsUfsCache.put(rootPath, new HdfsUnderFileSystem(path, tachyonConf, conf));
      }
      return mHdfsUfsCache.get(rootPath);
    }
  }

  private static Path getRoot(Path path) {
    Path currPath = path;
    while (currPath.getParent() != null) {
      currPath = currPath.getParent();
    }
    return currPath;
  }

  @Override
  public boolean supportsPath(String path, TachyonConf conf) {
    if (path == null) {
      return false;
    }

    return UnderFileSystem.isHadoopUnderFS(path, conf);
  }
}

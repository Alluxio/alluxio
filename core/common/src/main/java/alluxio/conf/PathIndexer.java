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

package alluxio.conf;

import alluxio.AlluxioURI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Finds the configured index of the path pattern that matches a specified path.
 */
public final class PathIndexer {
  /**
   * Returned by {@link #index(AlluxioURI)} when there is no matched path.
   */
  public static final int NO_INDEX = -1;

  private final Map<String, Integer> mIndex = new HashMap<>();
  private final PathMatcher mMatcher;

  /**
   * The expected properties in conf are of format:
   * {@code alluxio.path.<index>=<path pattern>}.
   *
   * The conf is not copied or stored in this class, later updates to conf will not update the
   * internal data structures.
   *
   * @param conf the Alluxio configuration
   */
  public PathIndexer(AlluxioConfiguration conf) {
    Set<String> paths = new HashSet<>();
    int n = conf.getInt(PropertyKey.PATHS);
    for (int i = 0; i < n; i++) {
      PropertyKey key = PropertyKey.Template.PATH_INDEX.format(i);
      String path = conf.get(key);
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }
      paths.add(path);
      mIndex.put(path, i);
    }
    mMatcher = new PrefixPathMatcher(paths);
  }

  /**
   * @param path the Alluxio path
   * @return the index of the matched path pattern or {@link NOT_FOUND} if there is no match
   */
  public int index(String path) {
    String match = mMatcher.match(path);
    if (match.equals(PathMatcher.NO_MATCH)) {
      return NO_INDEX;
    }
    return mIndex.get(match);
  }
}

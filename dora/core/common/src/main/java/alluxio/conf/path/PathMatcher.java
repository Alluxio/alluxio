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

package alluxio.conf.path;

import alluxio.AlluxioURI;

import java.util.List;
import java.util.Optional;

/**
 * Matches a path against a set of path patterns.
 */
public interface PathMatcher {
  /**
   * @param path the path to be matched, must start with "/"
   * @return the list of matched path patterns sorted by descending match preferences
   */
  Optional<List<String>> match(AlluxioURI path);
}

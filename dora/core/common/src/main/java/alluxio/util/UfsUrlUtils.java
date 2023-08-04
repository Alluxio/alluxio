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

package alluxio.util;

import com.google.common.base.Preconditions;

public final class UfsUrlUtils {

  public static final String PATH_SEPARATOR = "/";
  
  private UfsUrlUtils() {}  // prevent instantiation
  
  public static String concatStringPath(String pathA, String pathB) {
    Preconditions.checkNotNull(pathA);
    Preconditions.checkNotNull(pathB);
    if (pathA.endsWith(PATH_SEPARATOR) && pathB.startsWith(PATH_SEPARATOR)) {
      return pathA.substring(0, pathA.length() - 1) + pathB;
    } else if (!pathA.endsWith(PATH_SEPARATOR) && !pathB.startsWith(PATH_SEPARATOR)) {
      return pathA + PATH_SEPARATOR + pathB;
    } else {
      return pathA + pathB;
    }
  }
}

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

package tachyon.underfs.local;

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;
import tachyon.util.OSUtils;

/**
 * Factory for {@link LocalUnderFileSystem} instances
 */
public class LocalUnderFileSystemFactory implements UnderFileSystemFactory {

  @Override
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    return new LocalUnderFileSystem(tachyonConf);
  }

  @Override
  public boolean supportsPath(String path, TachyonConf tachyonConf) {
    if (path == null) {
      return false;
    }
    return path.startsWith(TachyonURI.SEPARATOR)
        || path.startsWith("file://")
        || hasWindowsDrive(path, false);
  }

  /**
   * Check if the path is a windows path.
   *
   * @param path the path to check
   * @param slashed if the path starts with a slash.
   * @return true if it is a windows path, false otherwise
   */
  private boolean hasWindowsDrive(String path, boolean slashed) {
    int start = slashed ? 1 : 0;
    return OSUtils.isWindows()
        && path.length() >= start + 2
        && (!slashed || path.charAt(0) == '/')
        && path.charAt(start + 1) == ':'
        && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a'
        && path.charAt(start) <= 'z'));
  }
}

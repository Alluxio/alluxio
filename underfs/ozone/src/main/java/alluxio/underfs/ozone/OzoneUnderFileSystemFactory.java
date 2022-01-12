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

package alluxio.underfs.ozone;

import alluxio.Constants;
import alluxio.OzoneUfsConstants;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystemFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link HdfsUnderFileSystem}.
 *
 * It caches created {@link HdfsUnderFileSystem}s, using the scheme and authority pair as the key.
 */
@ThreadSafe
public class OzoneUnderFileSystemFactory extends HdfsUnderFileSystemFactory {

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_OZONE);
  }

  @Override
  public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
    if (supportsPath(path)) {
      if (!conf.isSet(PropertyKey.UNDERFS_VERSION)
          || conf.get(PropertyKey.UNDERFS_VERSION).equals(getVersion())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getVersion() {
    return OzoneUfsConstants.UFS_OZONE_VERSION;
  }
}

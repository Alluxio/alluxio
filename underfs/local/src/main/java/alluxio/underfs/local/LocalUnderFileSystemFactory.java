/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.local;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link LocalUnderFileSystem}.
 */
@ThreadSafe
public class LocalUnderFileSystemFactory implements UnderFileSystemFactory {

  @Override
  public UnderFileSystem create(String path, Configuration configuration, Object ufsConf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    return new LocalUnderFileSystem(new AlluxioURI(path), configuration);
  }

  @Override
  public boolean supportsPath(String path, Configuration configuration) {
    if (path == null) {
      return false;
    }
    return path.startsWith(AlluxioURI.SEPARATOR)
        || path.startsWith("file://")
        || AlluxioURI.hasWindowsDrive(path, false);
  }
}

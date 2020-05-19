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

package alluxio.underfs.b2;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link B2UnderFileSystem}.
 */
@ThreadSafe
public class B2UnderFileSystemFactory implements UnderFileSystemFactory {

  private void checkB2Credentials(UnderFileSystemConfiguration conf) {
    if (!conf.keySet().contains(PropertyKey.B2_ACCESS_KEY) || !conf.keySet()
        .contains(PropertyKey.B2_SECRET_KEY)) {
      String err = "B2 Credentials not available, cannot create B2 Under File System. "
          + "The following properties should be set:\n"
          + "\t- " + PropertyKey.Name.B2_ACCESS_KEY
          + "\t- " + PropertyKey.Name.B2_SECRET_KEY;
      throw new RuntimeException(new IOException(err));
    }
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path is null");
    checkB2Credentials(conf);
    try {
      return B2UnderFileSystem.createInstance(new AlluxioURI(path), conf);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    if (path == null) {
      return false;
    }
    return path.startsWith(B2UnderFileSystem.B2_SCHEME);
  }

  @Override
  public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
    return supportsPath(path);
  }
}

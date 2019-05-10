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

package alluxio.underfs.kodo;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link KodoUnderFileSystem}.
 */
public class KodoUnderFileSystemFactory implements UnderFileSystemFactory {
  /**
   * Constructs a new {@link KodoUnderFileSystem}.
   */
  public KodoUnderFileSystemFactory() {}

  /**
   *
   * @param path file path
   * @param conf optional configuration object for the UFS, may be null
   * @return
   */
  @Override
  public UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    return KodoUnderFileSystem.creatInstance(new AlluxioURI(path), conf);
  }

  /**
   *
   * @param path file path
   * @return
   */
  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_KODO);
  }
}

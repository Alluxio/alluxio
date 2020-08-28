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

package alluxio.testutils.underfs.delegating;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

/**
 * Factory for delegating under file systems.
 */
public class DelegatingUnderFileSystemFactory implements UnderFileSystemFactory {
  public static final String DELEGATING_SCHEME = "delegating";

  private final UnderFileSystem mUfs;

  /**
   * Creates a factory which returns the given ufs.
   *
   * @param ufs the ufs to return from calls to create
   */
  public DelegatingUnderFileSystemFactory(UnderFileSystem ufs) {
    mUfs = ufs;
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    return mUfs;
  }

  @Override
  public boolean supportsPath(String path) {
    return path.startsWith(DELEGATING_SCHEME);
  }
}

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

package alluxio;

import alluxio.underfs.DirectoryUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;

import org.junit.Assert;

import java.io.IOException;

/**
 * Util methods for writing UFS integration tests.
 */
public final class UfsIntegrationTestUtils {
  
  public static void mkdirsIfSupported(UnderFileSystem ufs, String path, MkdirsOptions options)
      throws IOException {
    if (ufs instanceof DirectoryUnderFileSystem) {
      DirectoryUnderFileSystem directoryUfs = (DirectoryUnderFileSystem) ufs;
      directoryUfs.mkdirs(path, options);
    }
  }

  public static void checkDirIfSupported(UnderFileSystem ufs, String path, boolean expectedValue)
      throws IOException {
    if (ufs instanceof DirectoryUnderFileSystem) {
      DirectoryUnderFileSystem directoryUfs = (DirectoryUnderFileSystem) ufs;
      Assert.assertEquals(expectedValue, directoryUfs.isDirectory(path));
    }
  }

  private UfsIntegrationTestUtils() {} // This is a utils class not intended for instantiation
}

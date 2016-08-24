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

package alluxio.underfs.swift;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SwiftUnderFileSystem}.
 */
public class SwiftUnderFileSystemFactoryTest {

  /**
   * This test ensures the Swift UFS module correctly accepts paths that begin with swift://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("swift://localhost/test/path");
    UnderFileSystemFactory factory2 = UnderFileSystemRegistry.find("file://localhost/test/path");

    Assert.assertNotNull("A UnderFileSystemFactory should exist for swift paths when using this "
        + "module", factory);
    Assert.assertNull("A UnderFileSystemFactory should not exist for non supported paths when "
        + "using this module", factory2);
  }
}

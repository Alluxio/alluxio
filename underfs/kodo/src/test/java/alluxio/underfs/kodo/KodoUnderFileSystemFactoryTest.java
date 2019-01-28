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

import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KodoUnderFileSystemFactoryTest}.
 */
public class KodoUnderFileSystemFactoryTest {

  /**
   * Tests that the KODO UFS module correctly accepts paths that begin with kodo://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find("kodo://test-bucket/path",
        ConfigurationTestUtils.defaults());

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for oss paths when using this module", factory);
  }
}

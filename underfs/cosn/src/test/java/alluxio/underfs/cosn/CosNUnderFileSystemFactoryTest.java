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

package alluxio.underfs.cosn;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link CosNUnderFileSystemFactory}.
 */
public class CosNUnderFileSystemFactoryTest {

  /**
   * Tests that the COSN UFS module correctly accepts paths that begin with cosn://.
   */
  @Test
  public void factory() {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry
                            .find("cosn://test-bucket/path", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for cosn paths when using this module", factory);
  }
}

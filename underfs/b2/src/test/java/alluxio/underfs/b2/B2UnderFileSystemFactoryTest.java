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

import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link B2UnderFileSystemFactory}.
 */
public class B2UnderFileSystemFactoryTest {

  /**
   * This test ensures the B2 UFS module correctly accepts paths that begin with b2://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry
        .find("b2://test-bucket/path", UnderFileSystemConfiguration.defaults());

    Assert
        .assertNotNull("A UnderFileSystemFactory should exist for b2 paths when using this module",
            factory);
  }
}

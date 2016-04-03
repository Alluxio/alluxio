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

package alluxio.underfs.gcs;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link GCSUnderFileSystemFactory}.
 */
public final class GCSUnderFileSystemFactoryTest {

  /**
   * This test ensures the GCS UFS module correctly accepts paths that begin with gs://.
   */
  @Test
  public void factoryTest() {
    Configuration conf = new Configuration();

    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("gs://test-bucket/path", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for gs paths when using this module", factory);
  }
}

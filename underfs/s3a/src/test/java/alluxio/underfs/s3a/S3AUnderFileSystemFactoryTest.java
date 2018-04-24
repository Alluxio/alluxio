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

package alluxio.underfs.s3a;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link S3AUnderFileSystemFactory}.
 */
public class S3AUnderFileSystemFactoryTest {

  /**
   * This test ensures the S3A UFS module correctly accepts paths that begin with s3a://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find("s3a://test-bucket/path");
    UnderFileSystemFactory factory2 = UnderFileSystemFactoryRegistry.find("s3n://test-bucket/path");
    UnderFileSystemFactory factory3 = UnderFileSystemFactoryRegistry.find("s3://test-bucket/path");

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for s3a paths when using this module", factory);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for non s3a paths when using this module",
        factory2);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for non s3a paths when using this module",
        factory3);
  }
}

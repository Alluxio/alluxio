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

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link S3AUnderFileSystemFactory}.
 */
public class S3AUnderFileSystemFactoryTest {

  @Test
  public void factory() {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find("s3a://test-bucket/path",
        conf);
    UnderFileSystemFactory factory2 = UnderFileSystemFactoryRegistry.find("s3://test-bucket/path",
        conf);
    UnderFileSystemFactory factory3 = UnderFileSystemFactoryRegistry.find("s3n://test-bucket/path",
        conf);

    Assert.assertNotNull(factory);
    Assert.assertNotNull(factory2);
    Assert.assertNull(factory3);
  }
}

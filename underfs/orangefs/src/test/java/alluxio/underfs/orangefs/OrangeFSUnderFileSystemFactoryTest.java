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

package alluxio.underfs.orangefs;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link OrangeFSUnderFileSystemFactory}.
 */
public class OrangeFSUnderFileSystemFactoryTest {

  /**
   * This test ensures the OrangeFS UFS module correctly accepts paths that begin with ofs://.
   */
  @Test
  public void factoryTest() {
    Configuration conf = new Configuration();

    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("ofs://localhost:3334/test/path", conf);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for OrangeFS paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("s3://localhost/test/path", conf);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path", conf);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("alluxio://localhost:19999/test", conf);
    Assert.assertNull("A UnderFileSystemFactory should not exist for non supported paths when "
        + "using this module", factory);
  }
}

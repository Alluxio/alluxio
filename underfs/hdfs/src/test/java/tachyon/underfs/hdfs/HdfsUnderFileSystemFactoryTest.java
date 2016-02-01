/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs.hdfs;

import org.junit.Assert;
import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystemFactory;
import tachyon.underfs.UnderFileSystemRegistry;

/**
 * Unit tests for the {@link HdfsUnderFileSystemFactory}.
 */
public class HdfsUnderFileSystemFactoryTest {

  /**
   * This test ensures the HDFS UFS module correctly accepts paths that begin with hdfs://.
   */
  @Test
  public void factoryTest() {
    TachyonConf conf = new TachyonConf();

    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("hdfs://localhost/test/path", conf);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for HDFS paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("s3://localhost/test/path", conf);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path", conf);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemRegistry.find("tachyon://localhost:19999/test", conf);
    Assert.assertNull("A UnderFileSystemFactory should not exist for non supported paths when "
        + "using this module", factory);
  }
}

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

package alluxio.underfs.local;

import org.junit.Assert;
import org.junit.Test;

import alluxio.conf.TachyonConf;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

/**
 * Unit tests for the {@link LocalUnderFileSystemFactory}.
 */
public class LocalUnderFileSystemFactoryTest {

  /**
   * This test ensures the local UFS module correctly accepts paths that begin with / or file://.
   */
  @Test
  public void factoryTest() {
    TachyonConf conf = new TachyonConf();

    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/local/test/path", conf);
    UnderFileSystemFactory factory2 = UnderFileSystemRegistry.find("file://local/test/path", conf);
    UnderFileSystemFactory factory3 = UnderFileSystemRegistry.find("hdfs://test-bucket/path", conf);
    UnderFileSystemFactory factory4 = UnderFileSystemRegistry.find("R:\\ramfs\\", conf);
    UnderFileSystemFactory factory5 = UnderFileSystemRegistry.find("file://R:/famfs", conf);
    UnderFileSystemFactory factory6 = UnderFileSystemRegistry.find("R:/ramfs/", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory2);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for non local paths when using this module",
        factory3);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory4);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory5);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory6);
  }
}

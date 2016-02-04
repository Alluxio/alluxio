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

package alluxio.underfs.oss;

import org.junit.Assert;
import org.junit.Test;

import alluxio.conf.TachyonConf;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

/**
 * Unit tests for the {@link OSSUnderFileSystemFactory}.
 */
public class OSSUnderFileSystemFactoryTest {

  /**
   * Tests that the OSS UFS module correctly accepts paths that begin with oss://.
   */
  @Test
  public void factoryTest() {
    TachyonConf conf = new TachyonConf();

    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("oss://test-bucket/path", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for oss paths when using this module", factory);
  }
}

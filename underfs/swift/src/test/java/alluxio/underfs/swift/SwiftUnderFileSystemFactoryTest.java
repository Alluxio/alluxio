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

package alluxio.underfs.swift;

import org.junit.Assert;
import org.junit.Test;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

/**
 * Unit tests for the {@link SwiftUnderFileSystem}.
 */
public class SwiftUnderFileSystemFactoryTest {

  /**
   * This test ensures the Swift UFS module correctly accepts paths that begin with swift://.
   */
  @Test
  public void factoryTest() {
    Configuration conf = new Configuration();

    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("swift://localhost/test/path", conf);
    UnderFileSystemFactory factory2 =
        UnderFileSystemRegistry.find("file://localhost/test/path", conf);

    Assert.assertNotNull("A UnderFileSystemFactory should exist for swift paths when using this "
        + "module", factory);
    Assert.assertNull("A UnderFileSystemFactory should not exist for non supported paths when "
        + "using this module", factory2);
  }
}

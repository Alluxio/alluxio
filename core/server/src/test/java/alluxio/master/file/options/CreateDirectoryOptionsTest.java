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

package alluxio.master.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import alluxio.conf.TachyonConf;

/**
 * Unit tests for {@link CreateDirectoryOptions}.
 */
public class CreateDirectoryOptionsTest {
  /**
   * Tests the {@link CreateDirectoryOptions.Builder}.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();

    CreateDirectoryOptions options = new CreateDirectoryOptions.Builder(new TachyonConf())
        .setAllowExists(allowExists)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setRecursive(recursive)
        .build();

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
  }

  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults();

    Assert.assertFalse(options.isAllowExists());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
  }
}

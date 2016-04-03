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

package alluxio.master.file.options;

import alluxio.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

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

    CreateDirectoryOptions options = new CreateDirectoryOptions.Builder(new Configuration())
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

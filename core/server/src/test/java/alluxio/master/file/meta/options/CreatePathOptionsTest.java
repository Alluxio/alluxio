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

package alluxio.master.file.meta.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link CreatePathOptions}.
 */
public class CreatePathOptionsTest {

  /**
   * Tests the {@link alluxio.master.file.meta.options.CreatePathOptions.Builder}.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    long blockSize = random.nextLong();
    boolean directory = random.nextBoolean();
    long operationTimeMs = random.nextLong();
    boolean persisted = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    long ttl = random.nextLong();

    CreatePathOptions options = new CreatePathOptions.Builder(new Configuration())
        .setAllowExists(allowExists)
        .setBlockSizeBytes(blockSize)
        .setDirectory(directory)
        .setOperationTimeMs(operationTimeMs)
        .setPersisted(persisted)
        .setRecursive(recursive)
        .setTtl(ttl)
        .build();

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(blockSize, options.getBlockSizeBytes());
    Assert.assertEquals(directory, options.isDirectory());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());
    Assert.assertEquals(persisted, options.isPersisted());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ttl, options.getTtl());
  }

  /**
   * Tests the {@link CreatePathOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, "64MB");
    MasterContext.reset(conf);

    CreatePathOptions options = CreatePathOptions.defaults();

    Assert.assertEquals(false, options.isAllowExists());
    Assert.assertEquals(64 * Constants.MB, options.getBlockSizeBytes());
    Assert.assertFalse(options.isDirectory());
    Assert.assertFalse(options.isPersisted());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(Constants.NO_TTL, options.getTtl());
    MasterContext.reset();
  }
}

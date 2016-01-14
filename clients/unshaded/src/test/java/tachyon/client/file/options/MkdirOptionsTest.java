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

package tachyon.client.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.UnderStorageType;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;

/**
 * Tests for the {@link MkdirOptions} class.
 */
public class MkdirOptionsTest {

  /**
   * Tests that building a {@link MkdirOptions} works.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    boolean allowExists = random.nextBoolean();
    boolean recursive = random.nextBoolean();
    UnderStorageType ufsType = UnderStorageType.SYNC_PERSIST;

    MkdirOptions options = new MkdirOptions.Builder(new TachyonConf())
        .setAllowExists(allowExists)
        .setRecursive(recursive)
        .setUnderStorageType(ufsType)
        .build();

    Assert.assertEquals(allowExists, options.isAllowExists());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(ufsType, options.getUnderStorageType());
  }

  /**
   * Tests that building a {@link MkdirOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    WriteType writeType = WriteType.CACHE_THROUGH;
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, writeType.toString());
    ClientContext.reset(conf);

    MkdirOptions options = MkdirOptions.defaults();

    Assert.assertFalse(options.isAllowExists());
    Assert.assertFalse(options.isRecursive());
    Assert.assertEquals(writeType.getUnderStorageType(), options.getUnderStorageType());
    ClientContext.reset();
  }
}

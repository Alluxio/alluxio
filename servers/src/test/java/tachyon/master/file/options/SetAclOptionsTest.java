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

package tachyon.master.file.options;

import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for the {@link SetAclOptions} class.
 */
public class SetAclOptionsTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests that building a {@link SetAclOptions} works.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    boolean recursive = random.nextBoolean();
    long operationTimeMs = random.nextLong();

    // owner
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String owner = new String(bytes);
    SetAclOptions options =
        new SetAclOptions.Builder()
            .setOwner(owner)
            .setRecursive(recursive)
            .setOperationTimeMs(operationTimeMs)
            .build();
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(recursive, options.isRecursive());
    Assert.assertEquals(operationTimeMs, options.getOperationTimeMs());

    // group
    random.nextBytes(bytes);
    String group = new String(bytes);
    options = new SetAclOptions.Builder()
        .setGroup(group)
        .setRecursive(recursive)
        .setOperationTimeMs(operationTimeMs)
        .build();
    Assert.assertEquals(group, options.getGroup());

    // permission
    short permission = (short) random.nextInt();
    options = new SetAclOptions.Builder()
        .setPermission(permission)
        .setRecursive(recursive)
        .setOperationTimeMs(operationTimeMs)
        .build();
    Assert.assertEquals(permission, options.getPermission());
  }

  @Test
  public void invalidOptionsTest() {
    mThrown.expect(IllegalArgumentException.class);

    new SetAclOptions.Builder().build();
  }
}

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
   * Tests setting the fields of a {@link SetAclOptions} object
   */
  @Test
  public void setFieldsTest() {
    Random random = new Random();
    boolean recursive = random.nextBoolean();

    // owner
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String owner = new String(bytes);
    SetAclOptions options = SetAclOptions.defaults().setOwner(owner).setRecursive(recursive);
    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(recursive, options.isRecursive());

    // group
    random.nextBytes(bytes);
    String group = new String(bytes);
    options = SetAclOptions.defaults().setGroup(group).setRecursive(recursive);
    Assert.assertEquals(group, options.getGroup());

    // permission
    short permission = (short) random.nextInt();
    options = SetAclOptions.defaults().setPermission(permission).setRecursive(recursive);
    Assert.assertEquals(permission, options.getPermission());
  }
}

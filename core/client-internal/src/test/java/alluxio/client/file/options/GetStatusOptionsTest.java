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

package alluxio.client.file.options;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link GetStatusOptions} class.
 */
public class GetStatusOptionsTest {
  /**
   * Tests that building a {@link GetStatusOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() {
    GetStatusOptions options = GetStatusOptions.defaults();
    Assert.assertFalse(options.isCheckUfs());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
    Random random = new Random();
    boolean checkUfs = random.nextBoolean();

    GetStatusOptions options = GetStatusOptions.defaults();
    options.setCheckUfs(checkUfs);

    Assert.assertEquals(checkUfs, options.isCheckUfs());
  }

}

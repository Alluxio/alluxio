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

import alluxio.Configuration;

/**
 * Unit tests for {@link SetAttributeOptions}.
 */
public class SetAttributeOptionsTest {
  /**
   * Tests the {@link SetAttributeOptions.Builder}.
   */
  @Test
  public void builderTest() {
    Random random = new Random();
    Boolean pinned = random.nextBoolean();
    Long ttl = random.nextLong();
    Boolean persisted = random.nextBoolean();

    SetAttributeOptions options = new SetAttributeOptions.Builder(new Configuration())
        .setPinned(pinned)
        .setTtl(ttl)
        .setPersisted(persisted)
        .build();

    Assert.assertEquals(pinned, options.getPinned());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(persisted, options.getPersisted());
  }

  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();

    Assert.assertNull(options.getPinned());
    Assert.assertNull(options.getTtl());
    Assert.assertNull(options.getPersisted());
  }
}

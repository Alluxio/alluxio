/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.test.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link LoadMetadataOptions} class.
 */
public final class LoadMetadataOptionsTest {
  @Test
  public void defaults() {
    LoadMetadataOptions options = LoadMetadataOptions.defaults();
    Assert.assertEquals(false, options.isCreateAncestors());
    Assert.assertEquals(null, options.getUfsStatus());
    Assert.assertEquals(DescendantType.NONE, options.getLoadDescendantType());
  }

  @Test
  public void fields() {
    Random random = new Random();
    boolean isCreateAncestors = random.nextBoolean();
    LoadMetadataOptions options = LoadMetadataOptions.defaults();
    options.setCreateAncestors(isCreateAncestors);
    Assert.assertEquals(isCreateAncestors, options.isCreateAncestors());

    DescendantType loadDescendantType = DescendantType.ALL;
    options.setLoadDescendantType(loadDescendantType);
    Assert.assertEquals(loadDescendantType, options.getLoadDescendantType());

    loadDescendantType = DescendantType.ONE;
    options.setLoadDescendantType(loadDescendantType);
    Assert.assertEquals(loadDescendantType, options.getLoadDescendantType());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(LoadMetadataOptions.class);
  }
}

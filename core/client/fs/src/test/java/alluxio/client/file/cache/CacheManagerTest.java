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

package alluxio.client.file.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;

import org.junit.Test;

public class CacheManagerTest {
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Test
  public void factoryGet() throws Exception {
    CacheManager manager = CacheManager.Factory.get(mConf);
    assertEquals(manager, CacheManager.Factory.get(mConf));
  }

  @Test
  public void factoryCreate() throws Exception {
    CacheManager manager = CacheManager.Factory.create(mConf);
    assertNotEquals(manager, CacheManager.Factory.create(mConf));
  }

  @Test
  public void factoryClear() throws Exception {
    CacheManager manager = CacheManager.Factory.get(mConf);
    CacheManager.Factory.clear();
    assertNotEquals(manager, CacheManager.Factory.get(mConf));
  }
}

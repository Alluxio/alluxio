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

package alluxio.master.file.meta;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link UfsAbsentPathCache}.
 */
public class UfsAbsentPathCacheTest {
  /**
   * Resets the configuration.
   */
  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  @Test
  public void defaultAsyncPathThreads() throws Exception {
    UfsAbsentPathCache cache = UfsAbsentPathCache.Factory.create(null);
    Assert.assertTrue(cache instanceof AsyncUfsAbsentPathCache);
  }

  @Test
  public void noAsyncPathThreads() throws Exception {
    ServerConfiguration.set(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS, 0);
    UfsAbsentPathCache cache = UfsAbsentPathCache.Factory.create(null);
    Assert.assertTrue(cache instanceof NoopUfsAbsentPathCache);
  }

  @Test
  public void negativeAsyncPathThreads() throws Exception {
    ServerConfiguration.set(PropertyKey.MASTER_UFS_PATH_CACHE_THREADS, -1);
    UfsAbsentPathCache cache = UfsAbsentPathCache.Factory.create(null);
    Assert.assertTrue(cache instanceof NoopUfsAbsentPathCache);
  }
}

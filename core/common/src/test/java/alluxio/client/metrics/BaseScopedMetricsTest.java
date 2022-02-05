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

package alluxio.client.metrics;

import static org.junit.Assert.assertEquals;
import alluxio.client.quota.CacheScope;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

public abstract class BaseScopedMetricsTest {

  private ScopedMetrics mScopedMetrics;
  private static final CacheScope SCOPE1 = CacheScope.create("db.schema.table1");
  private static final CacheScope SCOPE2 = CacheScope.create("db.schema.table2");
  private static final CacheScope SCOPE3 = CacheScope.create("db.schema.table3");

  protected abstract ScopedMetrics createMetrics();

  @Before
  public void before() {
    mScopedMetrics = createMetrics();
  }

  @Test
  public void defaults() {
    assertEquals(0, mScopedMetrics.getAllCacheScopes().size());
    assertEquals(0, mScopedMetrics.getCount(CacheScope.GLOBAL, ScopedMetricKey.BYTES_IN_CACHE));
  }

  @Test
  public void inc() {
    mScopedMetrics.inc(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 2);
    assertEquals(2, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.inc(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_IN_CACHE));
  }

  @Test
  public void dec() {
    mScopedMetrics.dec(SCOPE1, ScopedMetricKey.BYTES_READ_EXTERNAL, 2);
    assertEquals(-2, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_READ_EXTERNAL));
    mScopedMetrics.dec(SCOPE1, ScopedMetricKey.BYTES_READ_EXTERNAL, 3);
    assertEquals(-5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_READ_EXTERNAL));
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_READ_EXTERNAL));
  }

  @Test
  public void allCacheScopes() {
    assertEquals(0, mScopedMetrics.getAllCacheScopes().size());
    mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE);
    assertEquals(ImmutableSet.of(SCOPE1), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.inc(SCOPE2, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.dec(SCOPE3, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2, SCOPE3), mScopedMetrics.getAllCacheScopes());
  }

  @Test
  public void switchOrClear() {
    mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE);
    mScopedMetrics.inc(SCOPE2, ScopedMetricKey.BYTES_READ_CACHE, 3);
    mScopedMetrics.dec(SCOPE3, ScopedMetricKey.BYTES_READ_EXTERNAL, 3);
    mScopedMetrics.switchOrClear();
    assertEquals(ImmutableSet.of(), mScopedMetrics.getAllCacheScopes());
    assertEquals(0, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_READ_CACHE));
    assertEquals(0, mScopedMetrics.getCount(SCOPE3, ScopedMetricKey.BYTES_READ_EXTERNAL));
  }
}

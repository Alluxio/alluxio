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

public class SegmentedScopedMetricsTest {
  public static final int NUM_OF_SEGMENTS = 4;
  private SegmentedScopedMetrics mScopedMetrics;
  private static final CacheScope SCOPE1 = CacheScope.create("db.schema.table1");
  private static final CacheScope SCOPE2 = CacheScope.create("db.schema.table2");
  private static final CacheScope SCOPE3 = CacheScope.create("db.schema.table3");

  @Before
  public void before() {
    mScopedMetrics = new SegmentedScopedMetrics(NUM_OF_SEGMENTS);
  }

  @Test
  public void defaults() {
    assertEquals(0, mScopedMetrics.getAllCacheScopes().size());
    assertEquals(0, mScopedMetrics.getCount(CacheScope.GLOBAL, ScopedMetricKey.BYTES_IN_CACHE));
  }

  @Test
  public void incAndSwitch() {
    mScopedMetrics.inc(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 2);
    mScopedMetrics.switchOrClear();
    assertEquals(2, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    mScopedMetrics.inc(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    assertEquals(5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    //drop to 3, because the value 2 in the first segments had been popped up
    assertEquals(3, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    //drop to 0, because the value 3 in the second segments had been popped up
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_IN_CACHE));
  }

  @Test
  public void decAndSwitch() {
    mScopedMetrics.dec(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 2);
    mScopedMetrics.switchOrClear();
    assertEquals(-2, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    mScopedMetrics.dec(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(-5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    assertEquals(-5, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    //drop to 3, because the value 2 in the first segments had been popped up
    assertEquals(-3, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    mScopedMetrics.switchOrClear();
    //drop to 0, because the value 3 in the second segments had been popped up
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_IN_CACHE));
  }

  @Test
  public void allCacheScopesAndSwitch() {
    assertEquals(0, mScopedMetrics.getAllCacheScopes().size());
    mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE);
    assertEquals(ImmutableSet.of(SCOPE1), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear();
    mScopedMetrics.inc(SCOPE2, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear();
    mScopedMetrics.dec(SCOPE3, ScopedMetricKey.BYTES_IN_CACHE, 3);
    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2, SCOPE3), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear();
    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2, SCOPE3), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear(); //scope1 popped up
    assertEquals(ImmutableSet.of(SCOPE2, SCOPE3), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear(); //scope2 popped up
    assertEquals(ImmutableSet.of(SCOPE3), mScopedMetrics.getAllCacheScopes());
    mScopedMetrics.switchOrClear(); //scope3 popped up
    assertEquals(ImmutableSet.of(), mScopedMetrics.getAllCacheScopes());
  }

  @Test
  public void clearAll() {
    mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE);
    mScopedMetrics.inc(SCOPE2, ScopedMetricKey.BYTES_READ_CACHE, 3);
    mScopedMetrics.dec(SCOPE3, ScopedMetricKey.BYTES_READ_EXTERNAL, 3);

    mScopedMetrics.switchOrClear();

    assertEquals(ImmutableSet.of(SCOPE1, SCOPE2, SCOPE3), mScopedMetrics.getAllCacheScopes());
    assertEquals(0, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    assertEquals(3, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_READ_CACHE));
    assertEquals(-3, mScopedMetrics.getCount(SCOPE3, ScopedMetricKey.BYTES_READ_EXTERNAL));

    //switch and clear all the segments
    for (int i = 0; i < NUM_OF_SEGMENTS; i++) {
      mScopedMetrics.switchOrClear();
    }

    assertEquals(ImmutableSet.of(), mScopedMetrics.getAllCacheScopes());
    assertEquals(0, mScopedMetrics.getCount(SCOPE1, ScopedMetricKey.BYTES_IN_CACHE));
    assertEquals(0, mScopedMetrics.getCount(SCOPE2, ScopedMetricKey.BYTES_READ_CACHE));
    assertEquals(0, mScopedMetrics.getCount(SCOPE3, ScopedMetricKey.BYTES_READ_EXTERNAL));
  }
}

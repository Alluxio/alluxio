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

import org.junit.Test;

/**
 * Tests for AlluxioSystemScopedMetrics.
 */
public class AlluxioSystemScopedMetricsTest extends BaseScopedMetricsTest {
  @Override
  protected ScopedMetrics createMetrics() {
    return new AlluxioSystemScopedMetrics();
  }

  @Test
  public void allCacheScopes() {
    //TODO(beinan) implement allCacheScopes for Alluxio System scoped metrics
  }

  @Test
  public void switchOrClear() {
    //TODO(beinan) implement switchOrClear for Alluxio System scoped metrics
  }

  @Test
  public void defaults() {
    //TODO(beinan) implement defaults tests for Alluxio System scoped metrics
  }
}

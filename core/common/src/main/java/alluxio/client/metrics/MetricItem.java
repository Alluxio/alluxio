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

import com.codahale.metrics.Counter;

/**
 * Metric item for each scope, might contains multiple keys defined in MetricKeyInScope.
 */
public class MetricItem {
  private final Counter[] mCounters;

  MetricItem() {
    mCounters = new Counter[MetricKeyInScope.values().length];
    for (int i = 0; i < MetricKeyInScope.values().length; i++) {
      mCounters[i] = new Counter();
    }
  }

  long inc(MetricKeyInScope key, long n) {
    mCounters[key.ordinal()].inc(n);
    return getCount(key);
  }

  long getCount(MetricKeyInScope key) {
    return mCounters[key.ordinal()].getCount();
  }
}

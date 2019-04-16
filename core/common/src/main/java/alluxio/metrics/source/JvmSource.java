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

package alluxio.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A Source which collects JVM metrics, including JVM memory usage, GC counts, GC times, etc.
 */
@ThreadSafe
public class JvmSource implements Source {
  private static final String JVM_SOURCE_NAME = "jvm";
  private final MetricRegistry mMetricRegistry;

  /**
   * Creates a new {@link JvmSource} and register all JVM metrics.
   */
  public JvmSource() {
    mMetricRegistry = new MetricRegistry();
    mMetricRegistry.registerAll(new GarbageCollectorMetricSet());
    mMetricRegistry.registerAll(new MemoryUsageGaugeSet());
  }

  @Override
  public String getName() {
    return JVM_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }
}

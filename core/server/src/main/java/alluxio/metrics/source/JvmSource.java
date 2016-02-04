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

package alluxio.metrics.source;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

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

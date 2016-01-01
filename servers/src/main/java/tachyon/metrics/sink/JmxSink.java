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

package tachyon.metrics.sink;

import java.util.Properties;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * A sink which listens for new metrics and exposes them as namespaces MBeans.
 */
public class JmxSink implements Sink {
  private JmxReporter mReporter;

  /**
   * Creates a new {@link JmxSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties
   * @param registry the metric registry to register
   */
  public JmxSink(Properties properties, MetricRegistry registry) {
    mReporter = JmxReporter.forRegistry(registry).build();
  }

  @Override
  public void start() {
    mReporter.start();
  }

  @Override
  public void stop() {
    mReporter.stop();
  }

  @Override
  public void report() {
  }
}

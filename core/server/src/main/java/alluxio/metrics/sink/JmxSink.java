/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics.sink;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A sink which listens for new metrics and exposes them as namespaces MBeans.
 */
@NotThreadSafe
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

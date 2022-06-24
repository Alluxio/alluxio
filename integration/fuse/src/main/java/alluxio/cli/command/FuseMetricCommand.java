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

package alluxio.cli.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.FileInfo;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets fuse related metrics.
 */
@ThreadSafe
public final class FuseMetricCommand extends AbstractFuseShellCommand {

  /**
   * @param fs filesystem instance from fuse command
   * @param conf configuration instance from fuse command
   */
  public FuseMetricCommand(FileSystem fs, AlluxioConfiguration conf) {
    super(fs, conf, "");
  }

  /**
   * Gets metric from metric system, only support numeric metric display now.
   *
   * @param path the path uri from fuse command
   * @param argv the metric name
   * @return the result of running the command
   */
  @Override
  public URIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {
    String metricName = String.join(".", argv);
    String fullMetricName = MetricsSystem.getMetricName(metricName);
    Metric metric = MetricsSystem.getMetricValue(fullMetricName);
    if (metric == null) {
      throw new InvalidArgumentException(String.format("No such metric %s found in system!",
          argv[0]));
    }
    long value = 0;
    switch (metric.getMetricType()) {
      case COUNTER:
        value = MetricsSystem.counter(metricName).getCount();
        break;
      case METER:
        value = MetricsSystem.meter(metricName).getCount();
        break;
      case TIMER:
        value = MetricsSystem.timer(metricName).getCount();
        break;
      default:
        throw new InvalidArgumentException("Unsupported metric type");
    }
    return new URIStatus(new FileInfo().setLength(value).setCompleted(true));
  }

  @Override
  public String getCommandName() {
    return "metrics";
  }

  /**
   * Get metric command usage.
   * @return the usage information
   */
  @Override
  public String getUsage() {
    return String.format("%s%s.metrics.[metric key name]", Constants.DEAFULT_FUSE_MOUNT,
        Constants.ALLUXIO_CLI_PATH);
  }

  @Override
  public String getDescription() {
    return "Metrics command is used to get some useful metric information.";
  }
}

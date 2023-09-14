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

package alluxio.cli.fsadmin.report;

import alluxio.client.metrics.MetricsMasterClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Prints Alluxio metrics information.
 */
public class MetricsCommand {
  private final MetricsMasterClient mMetricsMasterClient;
  private final PrintStream mPrintStream;
  private static final Logger LOG = LoggerFactory.getLogger(JobServiceMetricsCommand.class);

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param metricsMasterClient client to connect to metrics master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(MetricsMasterClient metricsMasterClient, PrintStream printStream) {
    mMetricsMasterClient = metricsMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    MetricsOutput metricsInfo = new MetricsOutput(mMetricsMasterClient.getMetrics());
    try {
      String json = objectMapper.writeValueAsString(metricsInfo);
      mPrintStream.println(json);
    } catch (JsonProcessingException e) {
      mPrintStream.println("Failed to convert metricsInfo output to JSON. "
          + "Check the command line log for the detailed error message.");
      LOG.error("Failed to output JSON object {}", metricsInfo);
      e.printStackTrace();
      return -1;
    }
    return 0;
  }
}

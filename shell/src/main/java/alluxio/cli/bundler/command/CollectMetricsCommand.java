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

package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.UnavailableException;
import alluxio.util.SleepUtils;
import alluxio.util.network.HttpUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Command to probe Alluxio metrics for a few times.
 * */
public class CollectMetricsCommand extends AbstractCollectInfoCommand {
  public static final String COMMAND_NAME = "collectMetrics";
  private static final Logger LOG = LoggerFactory.getLogger(CollectMetricsCommand.class);
  private static final int COLLECT_METRICS_INTERVAL = 3 * 1000;
  private static final int COLLECT_METRICS_TIMES = 3;
  private static final int COLLECT_METRICS_TIMEOUT = 5 * 1000;
  private static final String METRICS_SERVLET_PATH = "/metrics/json/";

  /**
   * Creates a new instance of {@link CollectMetricsCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectMetricsCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    StringWriter outputBuffer = new StringWriter();
    for (int i = 0; i < COLLECT_METRICS_TIMES; i++) {
      LocalDateTime now = LocalDateTime.now();
      String timeMsg = String.format("Collecting metrics at %s", dtf.format(now));
      LOG.info(timeMsg);
      outputBuffer.write(timeMsg);

      // Generate URL from config properties
      String masterAddr;
      try {
        masterAddr = mFsContext.getMasterAddress().getHostName();
      } catch (UnavailableException e) {
        String noMasterMsg = "No Alluxio master available. Skip metrics collection.";
        LOG.warn(noMasterMsg);
        outputBuffer.write(noMasterMsg);
        break;
      }
      String url = String.format("http://%s:%s%s", masterAddr,
              mFsContext.getClusterConf().get(PropertyKey.MASTER_WEB_PORT),
              METRICS_SERVLET_PATH);
      LOG.info(String.format("Metric address URL: %s", url));

      // Get metrics
      String metricsResponse;
      try {
        metricsResponse = getMetricsJson(url);
      } catch (Exception e) {
        // Do not break the loop since the HTTP failure can be due to many reasons
        // Return the error message instead
        LOG.error("Failed to get Alluxio metrics from URL %s. Exception is %s", url, e);
        metricsResponse =  String.format("Url: %s%nError: %s", url, e.getMessage());
      }
      outputBuffer.write(metricsResponse);

      // Write to file
      File outputFile = generateOutputFile(mWorkingDirPath,
              String.format("%s-%s", getCommandName(), i));
      FileUtils.writeStringToFile(outputFile, metricsResponse);

      // Wait for an interval
      SleepUtils.sleepMs(LOG, COLLECT_METRICS_INTERVAL);
    }

    // TODO(jiacheng): phase 2 consider outputting partial results in a finally block
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s.txt", getCommandName()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return 0;
  }

  @Override
  public String getUsage() {
    return "collectMetrics <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect Alluxio metrics";
  }

  /**
   * Probes Alluxio metrics json sink.
   * If the HTTP request fails, return the error content
   * instead of throwing an exception.
   *
   * @param url URL that serves Alluxio metrics
   * @return HTTP response in JSON string
   */
  public String getMetricsJson(String url) throws IOException {
    String responseJson = HttpUtils.get(url, COLLECT_METRICS_TIMEOUT);
    return String.format("Url: %s%nResponse: %s", url, responseJson);
  }
}

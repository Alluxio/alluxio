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

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.UnavailableException;
import alluxio.util.SleepUtils;
import alluxio.util.network.HttpUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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

  private static final String EXCLUDE_OPTION_NAME = "exclude-worker-metrics";
  private static final Option ONLY_MASTER_OPTION =
          Option.builder().required(false).longOpt(EXCLUDE_OPTION_NAME).hasArg(false)
                  .desc("only collect master metrics\n"
                          + "By default collect master metrics and all worker metrics.")
                  .build();
  // Class specific options are aggregated into CollectInfo with reflection
  public static final Options OPTIONS = new Options().addOption(ONLY_MASTER_OPTION);

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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    StringWriter outputBuffer = new StringWriter();
    for (int i = 0; i < COLLECT_METRICS_TIMES; i++) {
      LocalDateTime now = LocalDateTime.now();
      String masterMsg = String.format("Collecting master metrics at %s ", dtf.format(now));
      LOG.info(masterMsg);
      outputBuffer.write(masterMsg);
      writeMasterMetrics(outputBuffer, i);
      if (!cl.hasOption(EXCLUDE_OPTION_NAME)) {
        String workerMsg = String.format("Collecting worker metrics at %s ", dtf.format(now));
        LOG.info(workerMsg);
        outputBuffer.write(workerMsg);
        writeWorkerMetrics(outputBuffer, i);
      }
      // Wait for an interval
      SleepUtils.sleepMs(LOG, COLLECT_METRICS_INTERVAL);
    }

    // TODO(jiacheng): phase 2 consider outputting partial results in a finally block
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s.txt", getCommandName()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return 0;
  }

  private void writeMasterMetrics(StringWriter outputBuffer, int i) throws IOException {
    outputBuffer.write(masterMetrics(mFsContext));
    outputBuffer.write("\n");

    // Write to file
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s-master-%s", getCommandName(), i));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());
  }

  private void writeWorkerMetrics(StringWriter outputBuffer, int i) throws IOException {
    for (String metricsResponse: workerMetrics(mFsContext)) {
      outputBuffer.write(metricsResponse);
      outputBuffer.write("\n");
    }
    // Write to file
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s-worker-%s", getCommandName(), i));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());
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
   * Get master metrics.
   * @param fsContext for connecting to master
   * @return the string of master metrics in JSON format
   */
  public static String masterMetrics(FileSystemContext fsContext) {
    // Generate URL from config properties
    String masterAddr;
    try {
      masterAddr = fsContext.getMasterAddress().getHostName();
    } catch (UnavailableException e) {
      String noMasterMsg = "No Alluxio master available. Skip metrics collection.";
      LOG.warn(noMasterMsg);
      return noMasterMsg;
    }
    String url = String.format("http://%s:%s%s", masterAddr,
            fsContext.getClusterConf().get(PropertyKey.MASTER_WEB_PORT),
            METRICS_SERVLET_PATH);
    LOG.info(String.format("Metric address URL: %s", url));

    // Get metrics
    String metricsResponse;
    try {
      metricsResponse = getMetricsJson(url);
    } catch (Exception e) {
      // Do not break the loop since the HTTP failure can be due to many reasons
      // Return the error message instead
      LOG.error("Failed to get Alluxio master metrics from URL {}. Exception: ", url, e);
      metricsResponse =  String.format("{Url: \"%s\",%n\"Error\": %s}", url, e.getMessage());
    }
    return metricsResponse;
  }

  /**
   * Get metrics from each worker.
   * @param fsContext for connecting to master
   * @return a list of worker metrics in JSON format
   * @throws IOException
   */
  public static List<String> workerMetrics(FileSystemContext fsContext) throws IOException {
    List<String> metricsResponses = new ArrayList<>();
    // Generate URL from config properties
    List<BlockWorkerInfo> workers;
    try {
      workers = fsContext.getCachedWorkers();
    } catch (UnavailableException e) {
      String noWorkerMsg = "No Alluxio workers available. Skip metrics collection.";
      LOG.warn(noWorkerMsg);
      metricsResponses.add(noWorkerMsg);
      return metricsResponses;
    }
    for (BlockWorkerInfo worker : workers) {
      String workerAddress = worker.getNetAddress().getContainerHost().equals("")
          ? worker.getNetAddress().getHost() : worker.getNetAddress().getContainerHost();
      String url = String.format("http://%s:%s%s", workerAddress,
          fsContext.getClusterConf().get(PropertyKey.WORKER_WEB_PORT),
          METRICS_SERVLET_PATH);
      LOG.info(String.format("Metric address URL: %s", url));

      // Get metrics
      try {
        metricsResponses.add(getMetricsJson(url));
      } catch (Exception e) {
        // Do not break the loop since the HTTP failure can be due to many reasons
        // Return the error message instead
        LOG.error("Failed to get Alluxio worker metrics from URL {}. Exception: ", url, e);
        metricsResponses.add(String.format("{Url: \"%s\",%n\"Error\": %s}", url, e.getMessage()));
      }
    }
    return metricsResponses;
  }

  /**
   * Probes Alluxio metrics json sink.
   * If the HTTP request fails, return the error content
   * instead of throwing an exception.
   *
   * @param url URL that serves Alluxio metrics
   * @return HTTP response in JSON string
   */
  public static String getMetricsJson(String url) throws IOException {
    String responseJson = HttpUtils.get(url, COLLECT_METRICS_TIMEOUT);
    return String.format("{Url: \"%s\",%n\"Response\": %s}", url, responseJson);
  }
}

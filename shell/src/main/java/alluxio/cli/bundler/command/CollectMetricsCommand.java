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
import alluxio.util.ConfigurationUtils;
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
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
      masterMetrics(outputBuffer, i);
      if (!cl.hasOption(EXCLUDE_OPTION_NAME)) {
        String workerMsg = String.format("Collecting worker metrics at %s ", dtf.format(now));
        LOG.info(workerMsg);
        outputBuffer.write(workerMsg);
        workerMetrics(outputBuffer, i);
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

  private void masterMetrics(StringWriter outputBuffer, int i) throws IOException {
    List<InetSocketAddress> masterAddresses =
        ConfigurationUtils.getMasterRpcAddresses(mFsContext.getClusterConf());
    boolean masterHostsDistinct =
        masterAddresses.stream().map(InetSocketAddress::getHostName).distinct().count()
            == masterAddresses.size();

    // Generate URL from config properties
    InetSocketAddress primaryMasterAddr;
    try {
      primaryMasterAddr = mFsContext.getMasterAddress();
    } catch (UnavailableException e) {
      String noMasterMsg = "No Alluxio master available. Skip metrics collection.";
      LOG.warn(noMasterMsg);
      outputBuffer.write(noMasterMsg);
      return;
    }

    for (InetSocketAddress masterAddress: masterAddresses) {
      String url = String.format("http://%s:%s%s", masterAddress.getHostName(),
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
        LOG.error("Failed to get Alluxio master metrics from URL {}. Exception: ", url, e);
        metricsResponse =  String.format("Url: %s%nError: %s", url, e.getMessage());
        if (!masterAddress.equals(primaryMasterAddr)) {
          metricsResponse += "\n"
              + "Standby master metrics cannot be collected if the web server isn't enabled.";
        }
      }
      outputBuffer.write(metricsResponse);
      outputBuffer.write("\n");

      String masterName = masterHostsDistinct ? masterAddress.getHostName() :
          masterAddress.getHostName() + ":" + masterAddress.getPort();
      if (masterAddress.equals(primaryMasterAddr)) {
        masterName += "(PRIMARY)";
      }

      // Write to file
      File outputFile = generateOutputFile(mWorkingDirPath,
          String.format("%s-master-%s-%s", getCommandName(), masterName, i));
      FileUtils.writeStringToFile(outputFile, metricsResponse);
    }
  }

  private void workerMetrics(StringWriter outputBuffer, int i) throws IOException {
    // Generate URL from config properties
    List<BlockWorkerInfo> workers;
    try {
      workers = mFsContext.getCachedWorkers();
    } catch (UnavailableException e) {
      String noWorkerMsg = "No Alluxio workers available. Skip metrics collection.";
      LOG.warn(noWorkerMsg);
      outputBuffer.write(noWorkerMsg);
      return;
    }
    boolean workerHostsDistinct = workers.size()
        == workers.stream().map(it -> it.getNetAddress().getHost()).distinct().count();
    for (BlockWorkerInfo worker : workers) {
      String url = String.format("http://%s:%s%s", worker.getNetAddress().getHost(),
          worker.getNetAddress().getWebPort(),
          METRICS_SERVLET_PATH);
      LOG.info(String.format("Metric address URL: %s", url));

      // Get metrics
      String metricsResponse;
      try {
        metricsResponse = getMetricsJson(url);
      } catch (Exception e) {
        // Do not break the loop since the HTTP failure can be due to many reasons
        // Return the error message instead
        LOG.error("Failed to get Alluxio worker metrics from URL {}. Exception: ", url, e);
        metricsResponse =  String.format("Url: %s%nError: %s", url, e.getMessage());
      }
      outputBuffer.write(metricsResponse);
      outputBuffer.write("\n");

      String workerName = workerHostsDistinct ? worker.getNetAddress().getHost() :
          worker.getNetAddress().getHost() + ":" + worker.getNetAddress().getRpcPort();
      // Write to file
      File outputFile = generateOutputFile(mWorkingDirPath,
          String.format("%s-worker-%s-%s", getCommandName(), workerName, i));
      FileUtils.writeStringToFile(outputFile, metricsResponse);
    }
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

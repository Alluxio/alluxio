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

import alluxio.client.job.JobMasterClient;
import alluxio.grpc.JobMasterStatus;
import alluxio.grpc.NetAddress;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.StatusSummary;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Prints job service metric information.
 */
public class JobServiceMetricsCommand {

  private final JobMasterClient mJobMasterClient;
  private final PrintStream mPrintStream;
  private final String mDateFormatPattern;

  /**
   * Creates a new instance of {@link JobServiceMetricsCommand}.
   *
   * @param JobMasterClient client to connect to job master client
   * @param printStream stream to print job services metrics information to
   * @param dateFormatPattern the pattern to follow when printing the date
   */
  public JobServiceMetricsCommand(JobMasterClient JobMasterClient, PrintStream printStream,
      String dateFormatPattern) {
    mJobMasterClient = JobMasterClient;
    mPrintStream = printStream;
    mDateFormatPattern = dateFormatPattern;
  }

  public static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).ofPattern("yyyyMMdd-HHmmss")
          .withLocale(Locale.getDefault()).withZone(ZoneId.systemDefault());

  /**
   * Runs a job services report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode jobServiceInfo = mapper.createObjectNode();

    ArrayNode masterInfo = mapper.createArrayNode();
    List<JobMasterStatus> allMasterStatus = mJobMasterClient.getAllMasterStatus();
    for (JobMasterStatus masterStatus : allMasterStatus) {
      ObjectNode master = mapper.createObjectNode();
      master.put("Host", masterStatus.getMasterAddress().getHost());
      master.put("Port", masterStatus.getMasterAddress().getRpcPort());
      master.put("State", masterStatus.getState());
      master.put("Start Time", DATETIME_FORMAT.format(Instant.ofEpochMilli(masterStatus.getStartTime())));
      master.put("Version", masterStatus.getVersion().getVersion());
      master.put("Revision", masterStatus.getVersion().getRevision());
      masterInfo.add(master);
    }
    jobServiceInfo.set("Masters", masterInfo);

    ArrayNode workerInfo = mapper.createArrayNode();
    List<JobWorkerHealth> allWorkerHealth = mJobMasterClient.getAllWorkerHealth();
    for (JobWorkerHealth workerHealth : allWorkerHealth) {
      ObjectNode worker = mapper.createObjectNode();
      worker.put("Host", workerHealth.getHostname());
      worker.put("Version", workerHealth.getVersion().getVersion());
      worker.put("Revision", workerHealth.getVersion().getRevision());
      worker.put("Task Pool Size", workerHealth.getTaskPoolSize());
      worker.put("Unfinished Tasks", workerHealth.getUnfinishedTasks());
      worker.put("Active Tasks", workerHealth.getNumActiveTasks());
      ObjectNode lAverage = mapper.createObjectNode();
      List<Double> loadAverage = workerHealth.getLoadAverage();
      lAverage.put("1 minute", loadAverage.get(0));
      lAverage.put("5 minutes", loadAverage.get(1));
      lAverage.put("15 minutes", loadAverage.get(2));
      worker.set("Load Average", lAverage);
      workerInfo.add(worker);
    }
    jobServiceInfo.set("Workers", workerInfo);

    ArrayNode jobStatusInfo = mapper.createArrayNode();
    JobServiceSummary jobServiceSummary = mJobMasterClient.getJobServiceSummary();
    Collection<StatusSummary> jobStatusSummaries = jobServiceSummary.getSummaryPerStatus();
    for (StatusSummary statusSummary : jobStatusSummaries) {
      ObjectNode status = mapper.createObjectNode();
      status.put("Status", statusSummary.getStatus().toString());
      status.put("Count", statusSummary.getCount());
      jobStatusInfo.add(status);
    }
    jobServiceInfo.set("Job Status", jobStatusInfo);

    ObjectNode recentJobInfo = mapper.createObjectNode();
    recentJobInfo.set("Recent Modified", getJobInfos(jobServiceSummary.getRecentActivities()));
    recentJobInfo.set("Recent Failed", getJobInfos(jobServiceSummary.getRecentFailures()));
    recentJobInfo.set("Longest running", getJobInfos(jobServiceSummary.getLongestRunning()));
    jobServiceInfo.set(String.format("Top %s jobs", JobServiceSummary.RECENT_LENGTH), recentJobInfo);

    mPrintStream.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobServiceInfo));
    return 0;
  }

  private ArrayNode getJobInfos(List<JobInfo> jobInfos) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode result = mapper.createArrayNode();
    for (JobInfo jobInfo : jobInfos) {
      ObjectNode jInfo = mapper.createObjectNode();
      jInfo.put("Timestamp", CommonUtils.convertMsToDate(jobInfo.getLastUpdated(), mDateFormatPattern));
      jInfo.put("Id", jobInfo.getId());
      jInfo.put("Name", jobInfo.getName());
      jInfo.put("Status", jobInfo.getStatus().toString());
      result.add(jInfo);
    }
    return result;
  }
}

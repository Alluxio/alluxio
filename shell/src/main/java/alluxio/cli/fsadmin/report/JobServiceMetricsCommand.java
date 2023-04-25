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
    List<JobMasterStatus> allMasterStatus = mJobMasterClient.getAllMasterStatus();
    String masterFormat = getMasterInfoFormat(allMasterStatus);
    mPrintStream.printf(masterFormat, "Master Address", "State", "Start Time",
        "Version", "Revision");
    for (JobMasterStatus masterStatus : allMasterStatus) {
      NetAddress address = masterStatus.getMasterAddress();
      mPrintStream.printf(masterFormat,
          address.getHost() + ":" + address.getRpcPort(),
          masterStatus.getState(),
          DATETIME_FORMAT.format(Instant.ofEpochMilli(masterStatus.getStartTime())),
          masterStatus.getVersion().getVersion(),
          masterStatus.getVersion().getRevision());
    }
    mPrintStream.println();

    List<JobWorkerHealth> allWorkerHealth = mJobMasterClient.getAllWorkerHealth();
    String workerFormat = getWorkerInfoFormat(allWorkerHealth);
    mPrintStream.printf(workerFormat, "Job Worker", "Version", "Revision", "Task Pool Size",
        "Unfinished Tasks", "Active Tasks", "Load Avg");

    for (JobWorkerHealth workerHealth : allWorkerHealth) {
      mPrintStream.printf(workerFormat,
          workerHealth.getHostname(), workerHealth.getVersion().getVersion(),
          workerHealth.getVersion().getRevision(),
          workerHealth.getTaskPoolSize(), workerHealth.getUnfinishedTasks(),
          workerHealth.getNumActiveTasks(),
          StringUtils.join(workerHealth.getLoadAverage(), ", "));
    }
    mPrintStream.println();

    JobServiceSummary jobServiceSummary = mJobMasterClient.getJobServiceSummary();

    Collection<StatusSummary> jobStatusSummaries = jobServiceSummary.getSummaryPerStatus();

    for (StatusSummary statusSummary : jobStatusSummaries) {
      mPrintStream.print(String.format("Status: %-10s", statusSummary.getStatus()));
      mPrintStream.println(String.format("Count: %s", statusSummary.getCount()));
    }

    mPrintStream.println();
    mPrintStream.println(String.format("%s Most Recently Modified Jobs:",
        JobServiceSummary.RECENT_LENGTH));

    List<JobInfo> lastActivities = jobServiceSummary.getRecentActivities();
    printJobInfos(lastActivities);

    mPrintStream.println(String.format("%s Most Recently Failed Jobs:",
        JobServiceSummary.RECENT_LENGTH));

    List<JobInfo> lastFailures = jobServiceSummary.getRecentFailures();
    printJobInfos(lastFailures);

    mPrintStream.println(String.format("%s Longest Running Jobs:",
        JobServiceSummary.RECENT_LENGTH));

    List<JobInfo> longestRunning = jobServiceSummary.getLongestRunning();
    printJobInfos(longestRunning);

    return 0;
  }

  private String getMasterInfoFormat(List<JobMasterStatus> masters) {
    int maxNameLength = 16;
    if (masters.size() > 0) {
      maxNameLength = masters.stream().map(m -> m.getMasterAddress().getHost().length() + 6)
          .max(Comparator.comparing(Integer::intValue)).get();
    }
    // hostname:port + state + startTime + version + revision
    return "%-" + maxNameLength + "s %-8s %-16s %-32s %-8s%n";
  }

  private String getWorkerInfoFormat(List<JobWorkerHealth> workers) {
    int maxNameLength = 16;
    if (workers.size() > 0) {
      maxNameLength = workers.stream().map(w -> w.getHostname().length())
          .max(Comparator.comparing(Integer::intValue)).get();
    }
    int firstIndent = 16;
    if (firstIndent <= maxNameLength) {
      // extend first indent according to the longest worker name
      firstIndent = maxNameLength + 6;
    }

    // hostname + version + revision + poolSize + unfinishedTasks + activeTasks + loadAvg
    return "%-" + firstIndent + "s %-32s %-8s %-14s %-16s %-12s %s%n";
  }

  private void printJobInfos(List<JobInfo> jobInfos) {
    for (JobInfo jobInfo : jobInfos) {
      mPrintStream.print(String.format("Timestamp: %-30s",
          CommonUtils.convertMsToDate(jobInfo.getLastUpdated(), mDateFormatPattern)));
      mPrintStream.print(String.format("Id: %-20s", jobInfo.getId()));
      mPrintStream.print(String.format("Name: %-20s", jobInfo.getName()));
      mPrintStream.println(String.format("Status: %s", jobInfo.getStatus()));
    }
    mPrintStream.println();
  }
}

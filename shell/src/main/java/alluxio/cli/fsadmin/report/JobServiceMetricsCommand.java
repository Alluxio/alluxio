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
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.StatusSummary;
import alluxio.util.CommonUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

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

  /**
   * Runs a job services report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    List<JobWorkerHealth> allWorkerHealth = mJobMasterClient.getAllWorkerHealth();

    for (JobWorkerHealth workerHealth : allWorkerHealth) {
      mPrintStream.print(String.format("Worker: %-10s  ", workerHealth.getHostname()));
      mPrintStream.print(String.format("Task Pool Size: %-7s", workerHealth.getTaskPoolSize()));
      mPrintStream.print(String.format("Unfinished Tasks: %-7s",
          workerHealth.getUnfinishedTasks()));
      mPrintStream.print(String.format("Active Tasks: %-7s",
          workerHealth.getNumActiveTasks()));
      mPrintStream.println(String.format("Load Avg: %s",
          StringUtils.join(workerHealth.getLoadAverage(), ", ")));
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

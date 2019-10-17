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
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.StatusSummary;
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

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
    JobServiceSummary jobServiceSummary = mJobMasterClient.getJobServiceSummary();

    Collection<StatusSummary> jobStatusSummaries = jobServiceSummary.getSummaryPerStatus();

    for (StatusSummary statusSummary : jobStatusSummaries) {
      mPrintStream.print(String.format("Status: %-10s", statusSummary.getStatus()));
      mPrintStream.println(String.format("Count: %s", statusSummary.getCount()));
    }

    mPrintStream.println();
    mPrintStream.println("Last 10 Activities:");

    Collection<JobInfo> lastActivities = jobServiceSummary.getLastActivities();

    for (JobInfo lastActivity : lastActivities) {
      mPrintStream.print(String.format("Timestamp: %-30s",
        CommonUtils.convertMsToDate(lastActivity.getLastStatusChangeMs(), mDateFormatPattern)));
      mPrintStream.print(String.format("Job Id: %-20s", lastActivity.getJobId()));
      mPrintStream.println(String.format("Status: %s", lastActivity.getStatus()));
    }

    mPrintStream.println();
    mPrintStream.println("Last 10 Failures:");

    Collection<JobInfo> lastFailures = jobServiceSummary.getLastFailures();

    for (JobInfo lastFailure : lastFailures) {
      mPrintStream.print(String.format("Timestamp: %-30s",
        CommonUtils.convertMsToDate(lastFailure.getLastStatusChangeMs(), mDateFormatPattern)));
      mPrintStream.print(String.format("Job Id: %-20s", lastFailure.getJobId()));
      mPrintStream.println(String.format("Status: %s", lastFailure.getStatus()));
    }

    return 0;
  }
}

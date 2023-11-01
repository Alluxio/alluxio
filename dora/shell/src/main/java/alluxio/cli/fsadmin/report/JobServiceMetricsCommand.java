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
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Prints job service metric information.
 */
public class JobServiceMetricsCommand {

  private static final Logger LOG = LoggerFactory.getLogger(JobServiceMetricsCommand.class);
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
    ObjectMapper objectMapper = new ObjectMapper();
    List<JobMasterStatus> allMasterStatus = mJobMasterClient.getAllMasterStatus();
    List<JobWorkerHealth> allWorkerHealth = mJobMasterClient.getAllWorkerHealth();
    JobServiceSummary jobServiceSummary = mJobMasterClient.getJobServiceSummary();

    JobServiceOutput jobServiceInfo = new JobServiceOutput(
        allMasterStatus, allWorkerHealth, jobServiceSummary);
    try {
      String json = objectMapper.writeValueAsString(jobServiceInfo);
      mPrintStream.println(json);
    } catch (JsonProcessingException e) {
      mPrintStream.println("Failed to convert jobServiceInfo output to JSON. "
          + "Check the command line log for the detailed error message.");
      LOG.error("Failed to output JSON object {}", jobServiceInfo);
      e.printStackTrace();
      return -1;
    }
    return 0;
  }
}

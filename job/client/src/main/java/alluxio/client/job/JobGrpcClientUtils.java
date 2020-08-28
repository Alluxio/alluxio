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

package alluxio.client.job;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.job.JobMasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utils for interacting with the job service through a gRPC client.
 */
@ThreadSafe
public final class JobGrpcClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JobGrpcClientUtils.class);

  /**
   * @param jobId the job id
   * @param alluxioConf the Alluxio configuration
   * @param verbose if true, will return the detailed job info
   * @return the {@link JobInfo} for the job id
   */
  public static JobInfo getJobStatus(long jobId, AlluxioConfiguration alluxioConf,
      boolean verbose) throws IOException {
    try (final JobMasterClient client = JobMasterClient.Factory
        .create(JobMasterClientContext.newBuilder(ClientContext.create(alluxioConf)).build())) {
      if (verbose) {
        return client.getJobStatusDetailed(jobId);
      } else {
        return client.getJobStatus(jobId);
      }
    }
  }

  /**
   * Runs the specified job and waits for it to finish. If the job fails, it is retried the given
   * number of times. If the job does not complete in the given number of attempts, an exception
   * is thrown.
   *
   * @param config configuration for the job to run
   * @param attempts number of times to try running the job before giving up
   * @param alluxioConf Alluxio configuration
   * @return the job id of the job
   */
  public static long run(JobConfig config, int attempts, AlluxioConfiguration alluxioConf)
      throws InterruptedException {
    CountingRetry retryPolicy = new CountingRetry(attempts);
    String errorMessage = "";
    while (retryPolicy.attempt()) {
      long jobId;
      try (JobMasterClient client = JobMasterClient.Factory.create(
          JobMasterClientContext.newBuilder(ClientContext.create(alluxioConf)).build())) {
        jobId = client.run(config);
      } catch (Exception e) {
        // job could not be started, retry
        LOG.warn("Exception encountered when starting a job.", e);
        continue;
      }
      JobInfo jobInfo = waitFor(jobId, alluxioConf);
      if (jobInfo == null) {
        // job status could not be fetched, give up
        break;
      }
      if (jobInfo.getStatus() == Status.COMPLETED || jobInfo.getStatus() == Status.CANCELED) {
        return jobInfo.getId();
      }
      errorMessage = jobInfo.getErrorMessage();
      LOG.warn("Job {} failed to complete with attempt {}. error: {}",
          jobId, retryPolicy.getAttemptCount(), errorMessage);
    }
    throw new RuntimeException("Failed to successfully complete the job: " + errorMessage);
  }

  /**
   * @param jobId the ID of the job to wait for
   * @return the job info once it finishes or null if the status cannot be fetched
   */
  @Nullable
  private static JobInfo waitFor(final long jobId, AlluxioConfiguration alluxioConf)
      throws InterruptedException {
    try (final JobMasterClient client =
        JobMasterClient.Factory.create(JobMasterClientContext
            .newBuilder(ClientContext.create(alluxioConf)).build())) {
      return CommonUtils.waitForResult("Job to finish", ()-> {
        try {
          return client.getJobStatus(jobId);
        } catch (Exception e) {
          LOG.warn("Failed to get status for job (jobId={})", jobId, e);
          return null;
        }
      }, (jobInfo) -> {
          if (jobInfo != null) {
            switch (jobInfo.getStatus()) {
              case FAILED: // fall through
              case CANCELED: // fall through
              case COMPLETED:
                return true;
              case RUNNING: // fall through
              case CREATED:
                return false;
              default:
                throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
            }
          }
          return true;
        }, WaitForOptions.defaults().setInterval(1000));
    } catch (IOException e) {
      LOG.warn("Failed to close job master client: {}", e.toString());
      return null;
    } catch (TimeoutException e) {
      // Should never happen since we use the default timeout of "never".
      throw new IllegalStateException(e);
    }
  }

  private JobGrpcClientUtils() {} // prevent instantiation
}

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

import alluxio.Constants;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.job.JobMasterClientConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utils for interacting with the job service through a Thrift client.
 */
@ThreadSafe
public final class JobThriftClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JobThriftClientUtils.class);

  /**
   * Runs the specified job and waits for it to finish. If the job fails, it is retried the given
   * number of times. If the job does not complete in the given number of attempts, an exception
   * is thrown.
   *
   * @param config configuration for the job to run
   * @param attempts number of times to try running the job before giving up
   */
  public static void run(JobConfig config, int attempts) throws InterruptedException {
    CountingRetry retryPolicy = new CountingRetry(attempts);
    while (retryPolicy.attempt()) {
      long jobId;
      try (JobMasterClient client =
          JobMasterClient.Factory.create(JobMasterClientConfig.defaults())) {
        jobId = client.run(config);
      } catch (Exception e) {
        // job could not be started, retry
        LOG.warn("Exception encountered when starting a job.", e);
        continue;
      }
      JobInfo jobInfo = waitFor(jobId);
      if (jobInfo == null) {
        // job status could not be fetched, give up
        break;
      }
      if (jobInfo.getStatus() == Status.COMPLETED || jobInfo.getStatus() == Status.CANCELED) {
        return;
      }
      LOG.warn("Job {} failed to complete: {}", jobId, jobInfo.getErrorMessage());
    }
    throw new RuntimeException("Failed to successfully complete the job.");
  }

  /**
   * Convenience method for calling {@link #createProgressThread(long, PrintStream)} with an
   * interval of 2 seconds.
   *
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(PrintStream stream) {
    return createProgressThread(2 * Constants.SECOND_MS, stream);
  }

  /**
   * Creates a thread which will write "." to the given print stream at the given interval. The
   * created thread is not started by this method. The created thread will be daemonic and will
   * halt when interrupted.
   *
   * @param intervalMs the time interval in milliseconds between writes
   * @param stream the print stream to write to
   * @return the thread
   */
  public static Thread createProgressThread(final long intervalMs, final PrintStream stream) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          CommonUtils.sleepMs(intervalMs);
          if (Thread.interrupted()) {
            return;
          }
          stream.print(".");
        }
      }
    });
    thread.setDaemon(true);
    return thread;
  }

  /**
   * @param jobId the ID of the job to wait for
   * @return the job info for the job once it finishes or null if the job status cannot be fetched
   */
  private static JobInfo waitFor(final long jobId) throws InterruptedException {
    final AtomicReference<JobInfo> finishedJobInfo = new AtomicReference<>();
    try (final JobMasterClient client =
        JobMasterClient.Factory.create(JobMasterClientConfig.defaults())) {
      CommonUtils.waitFor("Job to finish", ()-> {
        JobInfo jobInfo;
        try {
          jobInfo = client.getStatus(jobId);
        } catch (Exception e) {
          LOG.warn("Failed to get status for job (jobId={})", jobId, e);
          return true;
        }
        switch (jobInfo.getStatus()) {
          case FAILED: // fall through
          case CANCELED: // fall through
          case COMPLETED:
            finishedJobInfo.set(jobInfo);
            return true;
          case RUNNING: // fall through
          case CREATED:
            return false;
          default:
            throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
        }
      }, WaitForOptions.defaults().setInterval(1000));
    } catch (IOException e) {
      LOG.warn("Failed to close job master client: {}", e.toString());
    } catch (TimeoutException e) {
      // Should never happen since we use the default timeout of "never".
      throw new IllegalStateException(e);
    }

    return finishedJobInfo.get();
  }

  private JobThriftClientUtils() {} // prevent instantiation
}

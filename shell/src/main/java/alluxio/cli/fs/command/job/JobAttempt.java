package alluxio.cli.fs.command.job;

import alluxio.client.job.JobMasterClient;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.retry.RetryPolicy;

import java.io.IOException;

public abstract class JobAttempt {
  protected final JobMasterClient mClient;
  protected final RetryPolicy mRetryPolicy;

  private Long mJobId;

  public JobAttempt(JobMasterClient client, RetryPolicy retryPolicy) {
    mClient = client;
    mRetryPolicy = retryPolicy;
  }

  public boolean run() {
    if (mRetryPolicy.attempt()) {
      mJobId = null;
      try {
        mJobId = mClient.run(getJobConfig());
      } catch (IOException e) {
        System.out.println(String.format("Failed to run job (jobId={})", mJobId, e));
        // Do nothing. This will be counted as a failed attempt
      }
      return true;
    }
    logFailed();
    return false;
  }

  /**
   * Returns the status of the job attempt.
   * @return True if finished successfully or cancelled, False if FAILED and should be retried,
   *              null if the status should be checked again later
   */
  public Status check() {
    if (mJobId == null) {
      return Status.FAILED;
    }

    JobInfo jobInfo;
    try {
      jobInfo = mClient.getJobStatus(mJobId);
    } catch (IOException e) {
      System.out.println(String.format("Failed to get status for job (jobId={})", mJobId, e));
      return Status.FAILED;
    }

    // This make an assumption that this job tree only goes 1 level deep
    boolean finished = true;
    for (JobInfo child : jobInfo.getChildren()) {
      if (!child.getStatus().isFinished()) {
        finished = false;
        break;
      }
    }

    if (finished) {
      if (jobInfo.getStatus().equals(Status.FAILED)) {
        logFailedAttempt(jobInfo);
      } else if (jobInfo.getStatus().equals(Status.COMPLETED)) {
        logCompleted();
      }
      return jobInfo.getStatus();
    }
    return Status.RUNNING;
  }

  protected abstract JobConfig getJobConfig();

  protected abstract void logFailedAttempt(JobInfo jobInfo);

  protected abstract void logFailed();

  protected abstract void logCompleted();
}

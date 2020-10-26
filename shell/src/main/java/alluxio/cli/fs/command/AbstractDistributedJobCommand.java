package alluxio.cli.fs.command;

import alluxio.ClientContext;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.job.wire.Status;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The base class for all the distributed job based {@link alluxio.cli.Command} classes.
 * It provides handling for submitting multiple jobs and handling retries of them.
 */
public abstract class AbstractDistributedJobCommand extends AbstractFileSystemCommand {
  private static final int DEFAULT_ACTIVE_JOBS = 1000;

  protected List<JobAttempt> mSubmittedJobAttempts;
  protected int mActiveJobs;
  protected final JobMasterClient mClient;

  protected AbstractDistributedJobCommand(FileSystemContext fsContext) {
    super(fsContext);
    mSubmittedJobAttempts = Lists.newArrayList();
    final ClientContext clientContext = mFsContext.getClientContext();
    mClient = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(clientContext).build());
    mActiveJobs = DEFAULT_ACTIVE_JOBS;
  }

  protected void drain() {
    while (!mSubmittedJobAttempts.isEmpty()) {
      waitJob();
    }
  }

  /**
   * Waits for at least one job to complete.
   */
  protected void waitJob() {
    AtomicBoolean removed = new AtomicBoolean(false);
    while (true) {
      mSubmittedJobAttempts = mSubmittedJobAttempts.stream().filter((jobAttempt) -> {
        Status check = jobAttempt.check();
        switch (check) {
          case CREATED:
          case RUNNING:
            return true;
          case CANCELED:
          case COMPLETED:
            removed.set(true);
            return false;
          case FAILED:
            removed.set(true);
            return false;
          default:
            throw new IllegalStateException(String.format("Unexpected Status: %s", check));
        }
      }).collect(Collectors.toList());
      if (removed.get()) {
        return;
      }
    }
  }
}

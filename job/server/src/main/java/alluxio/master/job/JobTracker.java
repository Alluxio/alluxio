package alluxio.master.job;

import alluxio.client.job.JobContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.meta.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.job.command.CommandManager;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import javax.annotation.Nullable;

@ThreadSafe
public class JobTracker {
  private static final Logger LOG = LoggerFactory.getLogger(JobTracker.class);

  private final long mCapacity;
  private final long mRetentionMs;

  private final ConcurrentHashMap<Long, JobCoordinator> mCoordinators;

  private final PriorityBlockingQueue<JobFinishEntry> mFinished;

  public JobTracker(long capacity, long retentionMs) {
    mCapacity = capacity;
    mRetentionMs = retentionMs;
    mCoordinators = new ConcurrentHashMap<>(0,
    0.95f, ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM));
    mFinished = new PriorityBlockingQueue<>();
  }

  public synchronized void statusChangeCallback(JobInfo jobInfo) {
    if (jobInfo == null) {
      return;
    }
    Status status = jobInfo.getStatus();
    int sizeBefore = mFinished.size();
    LOG.error("mFinished.put before - STATUS: {} - size: {} - id: {}", status,
        sizeBefore, jobInfo.getId());
    if (status.isFinished()) {
      if (!mFinished.add(new JobFinishEntry(jobInfo))) {
        LOG.error("failed to add to finished job queue");
      }
    }
    int sizeAfter = mFinished.size();
    LOG.error("mFinished.put after - STATUS: {} - size: {} - id: {}", status,
        sizeAfter, jobInfo.getId());
  }

  @Nullable
  public JobCoordinator getCoordinator(long coordinator) {
    return mCoordinators.get(coordinator);
  }

  public synchronized long addJob(JobConfig jobConfig, JobIdGenerator generator,
      CommandManager manager,
      JobServerContext ctx, List<WorkerInfo> workers) throws JobDoesNotExistException,
      ResourceExhaustedException {
    return doWithLimitedResource(() -> {
      long jobId = generator.getNewJobId();
      JobCoordinator jobCoordinator = JobCoordinator.create(manager, ctx,
          workers, jobId, jobConfig, this::statusChangeCallback);
      mCoordinators.put(jobId, jobCoordinator);
      return jobId;
    });
  }

  private synchronized long doWithLimitedResource(JobSubmissionRunnable<Long> r) throws JobDoesNotExistException,
      ResourceExhaustedException {
    return atomicWithResources(isFull -> {
      boolean canExecute = true;
      if (isFull) {
        canExecute = false; // coordinators is at max capacity
        // Try to clear the queue
        if (mFinished.isEmpty()) {
          // The job master is at full capacity and no job has finished.
          throw new ResourceExhaustedException(
              ExceptionMessage.JOB_MASTER_FULL_CAPACITY.getMessage(mCapacity));
        }
        for (JobFinishEntry oldestEntry = mFinished.peek(); mFinished.peek() != null; oldestEntry =
            mFinished.peek()) {
          if (oldestEntry == null) {
            break;
          }
          JobInfo oldestJob = oldestEntry.getJobInfo();
          long timeSinceCompletion = CommonUtils.getCurrentMs() - oldestEntry.getCreationTime();
          if (timeSinceCompletion < mRetentionMs) {
            // mFinishedJobs is sorted. Can't iterate to a job within retention policy
            break;
          }
          // Remove the top item since we know it's old enough now.
          // Concurrent items could possibly be inserted before calling this, but the probability
          // of time moving backwards is low, so we make the assumption that anything inserted into
          // this queue after the peeking the entry should always come afterwards.
          JobFinishEntry removed = mFinished.poll();
          // ===== LOGGING STATEMENTS =====
          if (removed != null) {
            LOG.error("mFinished.remove - JobId {}", removed.getJobInfo().getId());
            if (!removed.equals(oldestEntry)) {
              LOG.error("polled item did not equal peeked item - got {} - expected {}",
                  removed.getJobInfo().getId(), oldestJob.getId());
            }
          } else {
            LOG.error("mFinished.remove FAILED - expected JobId {}, but got {}",
                oldestJob.getId(), null);
            continue;
          }

          // ===== LOGGING STATEMENTS =====

          // This job status may have changed since it was inserted, so check with the coordinator
          // If it has changed since. This entry is valid and we cannot consider the job
          // "finished" if the status change time is different. Otherwise, if the status hasn't
          // changed, the job is still completed and we can remove it from the coordinator map
          if (removed.getCreationTime() == oldestJob.getLastStatusChangeMs()) {
            // What if it isn't in the map?
            int sizeBefore = mCoordinators.size();
            JobCoordinator jc = mCoordinators.remove(oldestJob.getId());
            if (jc != null) {
              LOG.error("mCoordinators.remove job id: {}", jc.getJobId());
              canExecute = true;
            } else {
              LOG.error("mIdToJobCoord.remove FAILED - expected jobId: {}, but got {} ",
                  oldestJob.getId(), null);
              LOG.error("this shouldn't have happened.");
            }
            int sizeAfter = mCoordinators.size();
            if (sizeBefore == sizeAfter) {
              LOG.error("big uh oh");
            }
          } else {
            LOG.error("Couldn't remove from coordinators");
          }
        }
      }
      if (canExecute) {
        return r.run();
      } else {
        throw new ResourceExhaustedException(
            ExceptionMessage.JOB_MASTER_FULL_CAPACITY.getMessage(mCapacity));
      }
    });
  }

  /**
   * Run a function against atomic resources
   * @param action
   */
  private <T> T atomicWithResources(CapacityConsumer<T> action) throws JobDoesNotExistException,
      ResourceExhaustedException {
    return action.run(mCoordinators.size() >= mCapacity);
  }

  public Iterable<Long> jobs() {
    return mCoordinators.keySet();
  }

  public Iterable<JobCoordinator> coordinators() {
    return mCoordinators.values();
  }



  @FunctionalInterface
  private interface JobSubmissionRunnable<V> {
    V run() throws JobDoesNotExistException, ResourceExhaustedException;
  }

  @FunctionalInterface
  private interface CapacityConsumer<T> {
    /**
     * @param v true if at capacity, false otherwise
     * @return
     * @throws ResourceExhaustedException
     */
    T run(Boolean v) throws JobDoesNotExistException, ResourceExhaustedException;
  }

  private static class JobFinishEntry implements Comparable<JobFinishEntry> {

    private final JobInfo mJobInfo;
    private final long mCreationTimeMs;
    public JobFinishEntry(JobInfo ji) {
      mJobInfo = ji;
      mCreationTimeMs = mJobInfo.getLastStatusChangeMs(); // copy into a new variable
    }

    @Override
    public int compareTo(JobFinishEntry other) {
      return Long.compare(mCreationTimeMs, other.mCreationTimeMs);
    }

    public long getCreationTime() {
      return mCreationTimeMs;
    }

    public JobInfo getJobInfo() {
      return mJobInfo;
    }
  }
}

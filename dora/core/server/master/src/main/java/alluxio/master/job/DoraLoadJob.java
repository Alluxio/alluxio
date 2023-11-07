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

package alluxio.master.job;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadFailure;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MultiDimensionalMetricsSystem;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Load job that loads a file or a directory into Alluxio.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class DoraLoadJob extends AbstractJob<DoraLoadJob.DoraLoadTask> {
  private static final Logger LOG = LoggerFactory.getLogger(DoraLoadJob.class);
  public static final String TYPE = "load";
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);

  // Job configurations
  private final String mLoadPath;
  private final OptionalLong mBandwidth;
  private final boolean mUsePartialListing;
  private final boolean mVerificationEnabled;

  // Job states
  private final Queue<LoadSubTask> mRetrySubTasksDLQ = new ArrayDeque<>();
  // Only the most recent 1k failures are persisted.
  // If more are needed, can turn on debug LOG for this class.
  private final Queue<Pair<LoadSubTask, String>> mRecentFailures =
      Queues.synchronizedQueue(EvictingQueue.create(1_000));
  private final Queue<Pair<LoadSubTask, String>> mRecentRetries =
      Queues.synchronizedQueue(EvictingQueue.create(1_000));
  private final Set<String> mFailedFiles = new ConcurrentHashSet<>();
  private final AtomicLong mSkippedBlocksCount = new AtomicLong();
  private final AtomicLong mScannedInodesCount = new AtomicLong();
  private final AtomicLong mProcessedInodesCount = new AtomicLong();
  private final AtomicLong mLoadedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mSkippedByteCount = new AtomicLong();
  private final AtomicLong mProcessingSubTasksCount = new AtomicLong();
  private final AtomicLong mRetrySubTasksCount = new AtomicLong();
  private final AtomicLong mTotalFinalFailureCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private final AtomicBoolean mPreparingTasks = new AtomicBoolean(false);
  private final UnderFileSystem mUfs;
  private final boolean mLoadMetadataOnly;
  private static final double FAILURE_RATIO_THRESHOLD = Configuration.getDouble(
      PropertyKey.MASTER_DORA_LOAD_JOB_TOTAL_FAILURE_RATIO_THRESHOLD);
  private static final int FAILURE_COUNT_THRESHOLD = Configuration.getInt(
      PropertyKey.MASTER_DORA_LOAD_JOB_TOTAL_FAILURE_COUNT_THRESHOLD);
  private static final int RETRY_DLQ_CAPACITY = Configuration.getInt(
      PropertyKey.MASTER_DORA_LOAD_JOB_RETRY_DLQ_CAPACITY);
  private final boolean mSkipIfExists;

  private final Optional<String> mFileFilterRegx;
  private final long mVirtualBlockSize = Configuration.getBytes(
      PropertyKey.DORA_READ_VIRTUAL_BLOCK_SIZE);
  private final LoadSubTaskIterator mLoadSubTaskIterator;
  private final int mNumReplica;
  private final long mJobStartTimestamp;
  private volatile OptionalLong mJobFinishTimestamp = OptionalLong.empty();
  private volatile Optional<String> mFailedFileSavedPath = Optional.empty();

  class LoadSubTaskIterator implements Iterator<LoadSubTask> {
    private LoadSubTaskIterator(Iterator<UfsStatus> ufsStatusIterator) {
      mUfsStatusIterator = ufsStatusIterator;
    }

    volatile Set<WorkerInfo> mWorkers = Collections.emptySet();
    Iterator<LoadSubTask> mCurrentUfsStatusSubTaskIterator = Collections.emptyIterator();
    Iterator<UfsStatus> mUfsStatusIterator;

    private List<LoadSubTask> generateSubTasksForFile(
        UfsStatus ufsStatus, Set<WorkerInfo> workers) {
      List<LoadSubTask> subTasks = new ArrayList<>();
      // add load metadata task
      LoadMetadataSubTask subTask = new LoadMetadataSubTask(ufsStatus, mVirtualBlockSize);
      subTasks.add(subTask);
      if (!mLoadMetadataOnly && ufsStatus.isFile()
          && ufsStatus.asUfsFileStatus().getContentLength() != 0) {
        long contentLength = ufsStatus.asUfsFileStatus().getContentLength();
        if (mVirtualBlockSize > 0) {
          int numBlocks = (int) (contentLength / mVirtualBlockSize) + 1;
          for (int i = 0; i < numBlocks; i++) {
            long offset = mVirtualBlockSize * i;
            long leftover = contentLength - offset;
            subTasks.add(new LoadDataSubTask(ufsStatus, mVirtualBlockSize, offset,
                Math.min(leftover, mVirtualBlockSize)));
          }
        }
        else {
          subTasks.add(new LoadDataSubTask(ufsStatus, mVirtualBlockSize, 0, contentLength));
        }
      }
      List<LoadSubTask> subTasksWithWorker =
          assignSubtasksToWorkers(subTasks, workers, mNumReplica);
      mTotalByteCount.addAndGet(
          subTasksWithWorker.stream().mapToLong(LoadSubTask::getLength).sum());
      mProcessingSubTasksCount.addAndGet(subTasksWithWorker.size());
      return subTasksWithWorker;
    }

    private List<LoadSubTask> assignSubtasksToWorkers(
        List<LoadSubTask> subTasks, Set<WorkerInfo> workers, int numReplica) {
      ImmutableList.Builder<LoadSubTask> replicaSubTasks = new ImmutableList.Builder<>();
      for (LoadSubTask subTask : subTasks) {
        List<WorkerInfo> pickedWorkers =
            mWorkerAssignPolicy.pickWorkers(subTask.asString(), workers, numReplica);
        for (int i = 0; i < numReplica; i++) {
          replicaSubTasks.add(subTask.copy().setWorkerInfo(pickedWorkers.get(i)));
        }
      }
      return replicaSubTasks.build();
    }

    public void updateWorkerList(Set<WorkerInfo> workers) {
      mWorkers = workers;
    }

    @Override
    public boolean hasNext() {
      return mCurrentUfsStatusSubTaskIterator.hasNext()
          || mUfsStatusIterator.hasNext();
    }

    @Override
    public LoadSubTask next() {
      if (mCurrentUfsStatusSubTaskIterator.hasNext()) {
        return mCurrentUfsStatusSubTaskIterator.next();
      }
      if (!mUfsStatusIterator.hasNext()) {
        throw new NoSuchElementException("No more load subtask");
      }
      UfsStatus ufsStatus = mUfsStatusIterator.next();
      mScannedInodesCount.incrementAndGet();
      List<LoadSubTask> subTasks = generateSubTasksForFile(ufsStatus, mWorkers);
      mCurrentUfsStatusSubTaskIterator = subTasks.listIterator();
      // A ufs status generates at least one subtask.
      return mCurrentUfsStatusSubTaskIterator.next();
    }
  }

  /**
   * Constructor.
   *
   * @param path                file path
   * @param user                user for authentication
   * @param jobId               job identifier
   * @param bandwidth           bandwidth
   * @param usePartialListing   whether to use partial listing
   * @param verificationEnabled whether to verify the job after loaded
   * @param loadMetadataOnly    if set to true, only metadata will be loaded without loading
   * @param skipIfExists        skip if exists
   * @param fileFilterRegx      the regx pattern string for file filter
   * @param ufsStatusIterator   ufsStatus iterable
   * @param ufs                 under file system
   * @param replica             replica
   */
  public DoraLoadJob(String path, Optional<String> user, String jobId, OptionalLong bandwidth,
      boolean usePartialListing, boolean verificationEnabled, boolean loadMetadataOnly,
      boolean skipIfExists, Optional<String> fileFilterRegx, Iterator<UfsStatus> ufsStatusIterator,
                     UnderFileSystem ufs, int replica) {
    super(user, jobId, new HashBasedWorkerAssignPolicy());
    mLoadPath = requireNonNull(path, "path is null");
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    Preconditions.checkArgument(replica >= 1, "numReplica should be at least 1");
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    mUfs = ufs;
    mLoadMetadataOnly = loadMetadataOnly;
    mSkipIfExists = skipIfExists;
    mFileFilterRegx = fileFilterRegx;
    mLoadSubTaskIterator = new LoadSubTaskIterator(ufsStatusIterator);
    mNumReplica = replica;
    mJobStartTimestamp = CommonUtils.getCurrentMs();
    LOG.info("DoraLoadJob for {} created.", path);
  }

  /**
   * Prepare next set of tasks waiting to be kicked off.
   * it is made sure only one thread should be calling this.
   * @param workers
   * @return list of DoraLoadTask
   */
  private List<DoraLoadTask> prepareNextTasks(Set<WorkerInfo> workers) {
    LOG.debug("Preparing next set of tasks for jobId:{}", mJobId);
    mLoadSubTaskIterator.updateWorkerList(workers);
    int workerNum = workers.size();
    ImmutableList.Builder<LoadSubTask> batchBuilder = ImmutableList.builder();

    // TODO(elega) Instead of immediate retry & retry in the end of the loading process,
    // we should attach a timestamp to each retrying subtask and only retry those that
    // have failed for a while to better handle worker downtime.
    for (int numSubTasks = 0; numSubTasks < BATCH_SIZE * workerNum; ++numSubTasks) {
      if (mLoadSubTaskIterator.hasNext()) {
        batchBuilder.add(mLoadSubTaskIterator.next());
      } else if (!mRetrySubTasksDLQ.isEmpty()) {
        batchBuilder.add(mRetrySubTasksDLQ.poll());
      } else {
        break;
      }
    }

    ImmutableList<LoadSubTask> subTasks = batchBuilder.build();
    Map<WorkerInfo, DoraLoadTask> workerToTaskMap = aggregateSubTasks(subTasks);
    if (workerToTaskMap.isEmpty()) {
      return Collections.unmodifiableList(new ArrayList<>());
    }
    List<DoraLoadTask> tasks = new ArrayList<>(workerToTaskMap.values());
    LOG.debug("prepared tasks:{}", tasks);
    return tasks;
  }

  private Map<WorkerInfo, DoraLoadTask> aggregateSubTasks(List<LoadSubTask> subTasks) {
    Map<WorkerInfo, DoraLoadTask> workerToTaskMap = new HashMap<>();
    for (LoadSubTask subtask : subTasks) {
      WorkerInfo pickedWorker = subtask.getWorkerInfo();
      Preconditions.checkNotNull(pickedWorker, "pickedWorker is null");
      DoraLoadTask task = workerToTaskMap.computeIfAbsent(pickedWorker,
          w -> {
            DoraLoadTask t = new DoraLoadTask();
            t.setMyRunningWorker(pickedWorker);
            t.setJob(this);
            return t;
          });
      task.addSubTask(subtask);
    }
    return workerToTaskMap;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mLoadPath;
  }

  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(mLoadPath).setType(TYPE).build();
  }

  /**
   * Get bandwidth.
   * @return the allocated bandwidth
   */
  public OptionalLong getBandwidth() {
    return mBandwidth;
  }

  /**
   * Set load state to FAILED with given reason.
   * @param reason failure exception
   */
  @Override
  public void failJob(AlluxioRuntimeException reason) {
    mJobFinishTimestamp = OptionalLong.of(CommonUtils.getCurrentMs());
    setJobState(JobState.FAILED, true);
    mFailedReason = Optional.of(reason);
    // Move all pending retry subtask to failed subtask set
    while (!mRetrySubTasksDLQ.isEmpty()) {
      addFileFailure(
          mRetrySubTasksDLQ.poll(),
          FailureReason.CANCELLED,
          "Retry cancelled due to job failure");
    }
    JOB_LOAD_FAIL.inc();
    LOG.info("Load Job {} fails with status: {}", mJobId, this);
    persistFailedFilesList();
  }

  private void persistFailedFilesList() {
    LOG.info("Starting persisting failed files...");
    String fileListDir =
        Configuration.getString(PropertyKey.MASTER_DORA_LOAD_JOB_FAILED_FILE_LIST_DIR);
    String startTime = new SimpleDateFormat("yyyy_MM_dd_HH:mm:ss").format(mStartTime);
    String fileName =
        (mLoadPath + "_" + startTime).replaceAll("[^a-zA-Z0-9-_\\.]", "_");
    try {
      Files.createDirectories(Paths.get(fileListDir));
    } catch (Exception e) {
      LOG.warn("Failed to create directory to store failed file list {}", fileListDir, e);
      return;
    }
    File output = new File(fileListDir, fileName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
      for (String path : mFailedFiles) {
        writer.write(path);
        writer.newLine();
      }
      LOG.info("Persisted the failed file list to {} successfully", output.getAbsolutePath());
      mFailedFileSavedPath = Optional.of(output.getAbsolutePath());
    } catch (Exception e) {
      LOG.warn("Failed to persist the failed file list to {}", fileName, e);
    }
  }

  @Override
  public void setJobSuccess() {
    mJobFinishTimestamp = OptionalLong.of(CommonUtils.getCurrentMs());
    setJobState(JobState.SUCCEEDED, true);
    JOB_LOAD_SUCCESS.inc();
    LOG.info("Load Job {} succeeds with status {}", mJobId, this);
  }

  /**
   * Add bytes to total loaded bytes.
   * @param bytes bytes to be added to total
   */
  @VisibleForTesting
  public void addLoadedBytes(long bytes) {
    mLoadedByteCount.addAndGet(bytes);
  }

  /**
   * Add files to retry.
   * @param reason the failure reason
   * @param message the error message
   * @param subTask subTask to be retried
   */
  @VisibleForTesting

  private void addSubTaskToRetryOrFail(LoadSubTask subTask, FailureReason reason, String message) {
    LOG.debug("Retry file {}", subTask);
    if (subTask.isRetry() || !isHealthy() || mRetrySubTasksDLQ.size() >= RETRY_DLQ_CAPACITY) {
      addFileFailure(subTask, reason, message);
      return;
    }
    subTask.setRetry(true);
    mRetrySubTasksDLQ.offer(subTask);
    mRetrySubTasksCount.incrementAndGet();
    mRecentRetries.add(new Pair<>(
        subTask, format("Reason: %s, message: %s", reason.name(), message)));
    MultiDimensionalMetricsSystem.DISTRIBUTED_LOAD_FAILURE.labelValues(
        reason.name(), Boolean.toString(false)).inc();
    LOAD_FAIL_COUNT.inc();
  }

  /**
   * Add failed files.
   * @param subTask the load subtask
   * @param reason the failure reason
   * @param message the error message
   */
  private void addFileFailure(LoadSubTask subTask, FailureReason reason, String message) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the first one.
    mFailedFiles.add(subTask.getUfsPath());
    mRecentFailures.add(new Pair<>(
        subTask, format("Reason: %s, message: %s", reason.name(), message)));
    mTotalFinalFailureCount.incrementAndGet();
    MultiDimensionalMetricsSystem.DISTRIBUTED_LOAD_FAILURE.labelValues(
        reason.name(), Boolean.toString(true)).inc();
  }

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    return (new LoadProgressReport(this, verbose)).getReport(format);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoraLoadJob that = (DoraLoadJob) o;
    return Objects.equal(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getDescription());
  }

  @Override
  public boolean isHealthy() {
    long totalFailureCount = mTotalFinalFailureCount.get();
    if (FAILURE_RATIO_THRESHOLD >= 1.0 || FAILURE_COUNT_THRESHOLD < 0) {
      return true;
    }
    return mState != JobState.FAILED
        && totalFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) totalFailureCount / mProcessingSubTasksCount.get() <= FAILURE_RATIO_THRESHOLD;
  }

  @Override
  public boolean isCurrentPassDone() {
    return !mLoadSubTaskIterator.hasNext() && mRetrySubTasksDLQ.isEmpty()
        && mRetryTaskList.isEmpty();
  }

  @Override
  public void initiateVerification() {
    // No op for now
  }

  @Override
  public List<DoraLoadTask> getNextTasks(Set<WorkerInfo> workers) {
    /* Both scheduler thread and worker thread will try to call getNextTasks,
    only one of them needs to do the preparation of next set of tasks and whoever
    wins will do the processjob and kick off those tasks.
     */
    List<DoraLoadTask> list = new ArrayList<>();
    if (mPreparingTasks.compareAndSet(false, true)) {
      try {
        Iterator<DoraLoadTask> it = mRetryTaskList.iterator();
        if (it.hasNext()) {
          DoraLoadTask task = it.next();
          LOG.debug("Re-submit retried DoraLoadTask:{} in getNextTasks.",
              task.getTaskId());
          list.add(task);
          it.remove();
          return Collections.unmodifiableList(list);
        }
        list = prepareNextTasks(workers);
        return Collections.unmodifiableList(list);
      } finally {
        mPreparingTasks.compareAndSet(true, false);
      }
    }
    return list;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("JobId", mJobId)
        .add("Path", mLoadPath)
        .add("User", mUser)
        .add("Bandwidth", mBandwidth)
        .add("UsePartialListing", mUsePartialListing)
        .add("VerificationEnabled", mVerificationEnabled)
        .add("RetrySubTasks", mRetrySubTasksDLQ)
        .add("FailedFiles", mFailedFiles)
        .add("StartTime", mStartTime)
        .add("SkippedFileCount", mSkippedBlocksCount)
        .add("ProcessedInodesCount", mProcessedInodesCount)
        .add("RetryTaskCount", mRetrySubTasksCount)
        .add("LoadedByteCount", mLoadedByteCount)
        .add("TotalFailureCount", mTotalFinalFailureCount)
        .add("SkippedByteCount", mSkippedByteCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.LoadJobEntry.Builder jobEntry = alluxio.proto.journal.Job.LoadJobEntry
        .newBuilder()
        .setLoadPath(mLoadPath)
        .setState(JobState.toProto(mState))
        .setPartialListing(mUsePartialListing)
        .setVerify(mVerificationEnabled)
        .setSkipIfExists(mSkipIfExists)
        .setReplicas(mNumReplica)
        .setJobId(mJobId);
    mFileFilterRegx.ifPresent(jobEntry::setFileFilterRegx);
    mUser.ifPresent(jobEntry::setUser);
    mBandwidth.ifPresent(jobEntry::setBandwidth);
    mEndTime.ifPresent(jobEntry::setEndTime);
    return Journal.JournalEntry
        .newBuilder()
        .setLoadJob(jobEntry.build())
        .build();
  }

  /**
   * Get duration in seconds.
   * @return job duration in seconds
   */
  @VisibleForTesting
  public long getDurationInSec() {
    return (mEndTime.orElse(System.currentTimeMillis()) - mStartTime) / 1000;
  }

  @Override
  public boolean processResponse(DoraLoadTask doraLoadTask) {
    try {
      long totalLoadedBytes = doraLoadTask.getSubTasks().stream()
                                          .map(LoadSubTask::getLength)
                                          .reduce(Long::sum)
                                          .orElse(0L);
      // what if timeout ? job needs to proactively check or task needs to be aware
      LoadFileResponse response = doraLoadTask.getResponseFuture().get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.warn(format("[DistributedLoad] Get failure from worker:%s, failed files:%s",
            doraLoadTask.getMyRunningWorker(), response.getFailuresList()));
        for (LoadFailure failure : response.getFailuresList()) {
          if (failure.getSubtask().hasLoadDataSubtask()) {
            totalLoadedBytes -= failure.getSubtask().getLoadDataSubtask().getLength();
          }
          LoadSubTask subTask = LoadSubTask.from(failure, mVirtualBlockSize);
          if (!failure.getRetryable()) {
            addSubTaskToRetryOrFail(subTask, FailureReason.WORKER_FAILED, failure.getMessage());
          } else {
            addFileFailure(subTask, FailureReason.WORKER_FAILED, failure.getMessage());
          }
        }
      }
      int totalLoadedInodes = (int) doraLoadTask.getSubTasks().stream()
          .filter(LoadSubTask::isLoadMetadata).count()
          - (int) response.getFailuresList().stream()
          .filter(i -> i.getSubtask().hasLoadMetadataSubtask()).count();
      if (!mLoadMetadataOnly) {
        addLoadedBytes(totalLoadedBytes - response.getBytesSkipped());
        LOAD_FILE_SIZE.inc(totalLoadedBytes);
        LOAD_RATE.mark(totalLoadedBytes);
        LOAD_RATE.mark(totalLoadedBytes);
      }
      mProcessedInodesCount.addAndGet(totalLoadedInodes - response.getNumSkipped());
      LOAD_FILE_COUNT.inc(totalLoadedInodes);
      mSkippedBlocksCount.addAndGet(response.getNumSkipped());
      mSkippedByteCount.addAndGet(response.getBytesSkipped());
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      Throwable cause = e.getCause();
      LOG.warn("exception when trying to get load response.", cause);
      for (LoadSubTask subTask : doraLoadTask.getSubTasks()) {
        AlluxioRuntimeException exception = AlluxioRuntimeException.from(cause);
        addSubTaskToRetryOrFail(subTask, FailureReason.WORKER_RPC_FAILED,
            exception.getStatus() + ":" + exception.getMessage());
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("[DistributedLoad] Task get canceled and will retry.", e);
      doraLoadTask.getSubTasks().forEach(it -> addSubTaskToRetryOrFail(
          it, FailureReason.CANCELLED, e.getMessage()));
      return true;
    }
    catch (InterruptedException e) {
      doraLoadTask.getSubTasks().forEach(it -> addSubTaskToRetryOrFail(
          it, FailureReason.INTERRUPTED, e.getMessage()));
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  @Override
  public void onWorkerUnavailable(DoraLoadTask task) {
    LOG.warn("Worker became unavailable: {}", task.getMyRunningWorker());
    for (LoadSubTask subTask: task.getSubTasks()) {
      addSubTaskToRetryOrFail(
          subTask, FailureReason.MEMBERSHIP_CHANGED, "Worker became unavailable");
    }
  }

  @Override
  public boolean hasFailure() {
    return !mFailedFiles.isEmpty();
  }

  /**
   * Is verification enabled.
   *
   * @return whether verification is enabled
   */
  @Override
  public boolean needVerification() {
    return false;
  }

  /**
   * Dora load task. It contains a list of subtasks. Each subtask would be executed concurrently
   * on a worker.
   */
  public class DoraLoadTask extends Task<LoadFileResponse> {
    protected List<LoadSubTask> mSubTasks;

    /**
     * Constructor.
     */
    public DoraLoadTask() {
      super(DoraLoadJob.this, DoraLoadJob.this.mTaskIdGenerator.incrementAndGet());
      mSubTasks = new ArrayList<>();
    }

    /**
     * Get files to load of this task.
     * @return list of UfsStatus
     */
    public List<LoadSubTask> getSubTasks() {
      return mSubTasks;
    }

    private void addSubTask(LoadSubTask subTask) {
      mSubTasks.add(subTask);
    }

    @Override
    protected ListenableFuture<LoadFileResponse> run(BlockWorkerClient workerClient) {
      LOG.debug("Start running task:{} on worker:{}", this, getMyRunningWorker());
      LoadFileRequest loadFileReqBuilder = constructRpcRequest();
      return workerClient.loadFile(loadFileReqBuilder);
    }

    private LoadFileRequest constructRpcRequest() {
      LoadFileRequest.Builder loadFileReqBuilder = LoadFileRequest.newBuilder();
      mSubTasks.stream().map(LoadSubTask::toProto).forEach(loadFileReqBuilder::addSubtasks);
      UfsReadOptions.Builder ufsReadOptions = UfsReadOptions
          .newBuilder()
          .setTag(mJobId)
          .setPositionShort(false);
      mUser.ifPresent(ufsReadOptions::setUser);
      loadFileReqBuilder.setOptions(ufsReadOptions);
      loadFileReqBuilder.setSkipIfExists(mSkipIfExists);
      return loadFileReqBuilder.build();
    }

    @Override
    public String toString() {
      final StringBuilder filesBuilder = new StringBuilder();
      getSubTasks().forEach(f -> {
        filesBuilder.append(f.asString() + ",");
      });
      return MoreObjects.toStringHelper(this)
          .add("taskJobType", mMyJob.getClass())
          .add("taskJobId", mMyJob.getJobId())
          .add("taskId", getTaskId())
          .add("taskFiles", filesBuilder.toString())
          .toString();
    }
  }

  private static class LoadProgressReport {
    private final boolean mVerbose;
    private final JobState mJobState;
    private final Long mBandwidth;
    private final boolean mVerificationEnabled;
    private final long mSkippedByteCount;
    private final long mLoadedByteCount;
    private final long mScannedInodesCount;
    private final long mProcessedInodesCount;
    private final Long mTotalByteCount;
    private final Long mThroughput;
    private final double mFailureFilesPercentage;
    private final double mFailureSubTasksPercentage;
    private final double mRetrySubTasksPercentage;

    private final AlluxioRuntimeException mFailureReason;
    private final long mFailedFileCount;
    private final List<Pair<LoadSubTask, String>> mRecentFailedSubtasksWithReasons;
    private final List<Pair<LoadSubTask, String>> mRecentRetryingSubtasksWithReasons;
    private final boolean mSkipIfExists;
    private final boolean mMetadataOnly;
    private String mRunningStage;
    private final int mRetryDeadLetterQueueSize;
    private final long mTimeElapsed;
    @Nullable private final String mFailedFileSavedPath;

    /**
     * Constructor.
     * @param job the job
     * @param verbose verbose
     */
    public LoadProgressReport(DoraLoadJob job, boolean verbose) {
      mVerbose = verbose;
      mJobState = job.mState;
      mBandwidth = job.mBandwidth.isPresent() ? job.mBandwidth.getAsLong() : null;
      mVerificationEnabled = job.mVerificationEnabled;
      mProcessedInodesCount = job.mProcessedInodesCount.get();
      mLoadedByteCount = job.mLoadedByteCount.get();
      if (!job.mUsePartialListing) {
        mTotalByteCount = job.mTotalByteCount.get();
      } else {
        mTotalByteCount = null;
      }
      long duration = job.getDurationInSec();
      if (duration > 0) {
        mThroughput = job.mLoadedByteCount.get() / duration;
      } else {
        mThroughput = null;
      }
      mFailureFilesPercentage =
          ((double) (job.mFailedFiles.size())
              / (job.mScannedInodesCount.get())) * 100;
      mFailureSubTasksPercentage =
          ((double) (job.mTotalFinalFailureCount.get())
              / (job.mProcessingSubTasksCount.get())) * 100;
      mRetrySubTasksPercentage =
          ((double) (job.mRetrySubTasksCount.get())
              / (job.mProcessingSubTasksCount.get())) * 100;
      mScannedInodesCount = job.mScannedInodesCount.get();
      mFailureReason = job.mFailedReason.orElse(null);
      mFailedFileCount = job.mFailedFiles.size();
      if (verbose) {
        if (!job.mRecentFailures.isEmpty()) {
          mRecentFailedSubtasksWithReasons = new ArrayList<>(job.mRecentFailures);
        } else {
          mRecentFailedSubtasksWithReasons = Collections.emptyList();
        }
        if (!job.mRecentRetries.isEmpty()) {
          mRecentRetryingSubtasksWithReasons = new ArrayList<>(job.mRecentRetries);
        } else {
          mRecentRetryingSubtasksWithReasons = Collections.emptyList();
        }
      } else {
        mRecentFailedSubtasksWithReasons = Collections.emptyList();
        mRecentRetryingSubtasksWithReasons = Collections.emptyList();
      }
      mSkippedByteCount = job.mSkippedByteCount.get();
      mSkipIfExists = job.mSkipIfExists;
      mMetadataOnly = job.mLoadMetadataOnly;
      mRunningStage = "";
      if (mJobState == JobState.RUNNING && verbose) {
        mRunningStage = job.mLoadSubTaskIterator.hasNext() ? "LOADING" : "RETRYING";
      }
      mRetryDeadLetterQueueSize = job.mRetrySubTasksDLQ.size();
      mTimeElapsed =
          job.mJobFinishTimestamp.orElse(CommonUtils.getCurrentMs()) - job.mJobStartTimestamp;
      mFailedFileSavedPath = job.mFailedFileSavedPath.orElse(null);
    }

    public String getReport(JobProgressReportFormat format)
    {
      switch (format) {
        case TEXT:
          return getTextReport();
        case JSON:
          return getJsonReport();
        default:
          throw new InvalidArgumentRuntimeException(
              format("Unknown load progress report format: %s", format));
      }
    }

    private String getTextReport() {
      StringBuilder progress = new StringBuilder();
      progress.append(
          format("\tSettings:\tbandwidth: %s\tverify: %s\tmetadata-only: %s%n",
              mBandwidth == null ? "unlimited" : mBandwidth,
              mVerificationEnabled, mMetadataOnly));
      progress.append(format("\tTime Elapsed: %s%n",
          DurationFormatUtils.formatDuration(mTimeElapsed, "HH:mm:ss")));
      progress.append(format("\tJob State: %s%s%n", mJobState,
          mFailureReason == null
              ? "" : format(
              " (%s: %s)",
              mFailureReason.getClass().getName(),
              mFailureReason.getMessage())));
      if (mJobState == JobState.RUNNING && mVerbose) {
        progress.append(format("\tStage: %s%n", mRunningStage));
      }
      if (mVerbose && mFailureReason != null) {
        for (StackTraceElement stack : mFailureReason.getStackTrace()) {
          progress.append(format("\t\t%s%n", stack.toString()));
        }
      }
      progress.append(format("\tInodes Scanned: %d%n", mScannedInodesCount));
      progress.append(format("\tInodes Processed: %d%n", mProcessedInodesCount));
      if (mSkipIfExists) {
        progress.append(format("\tBytes Skipped: %s%s%n",
            FormatUtils.getSizeFromBytes(mSkippedByteCount),
            mTotalByteCount == null
                ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      }

      if (!mMetadataOnly) {
        progress.append(format("\tBytes Loaded: %s%s%n",
            FormatUtils.getSizeFromBytes(mLoadedByteCount),
            mTotalByteCount == null
                ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
        if (mThroughput != null) {
          progress.append(format("\tThroughput: %s/s%n",
              FormatUtils.getSizeFromBytes(mThroughput)));
        }
      }
      progress.append(format("\tFile Failure rate: %.2f%%%n", mFailureFilesPercentage));
      progress.append(format("\tSubtask Failure rate: %.2f%%%n", mFailureSubTasksPercentage));
      progress.append(format("\tFiles Failed: %s%n", mFailedFileCount));
      if (mVerbose && mRecentFailedSubtasksWithReasons != null) {
        progress.append(format("\tRecent failed subtasks: %n"));
        mRecentFailedSubtasksWithReasons.forEach(pair ->
            progress.append(format("\t\t%s: %s%n", pair.getFirst(), pair.getSecond())));
        progress.append(format("\tRecent retrying subtasks: %n"));
        mRecentRetryingSubtasksWithReasons.forEach(pair ->
            progress.append(format("\t\t%s: %s%n", pair.getFirst(), pair.getSecond())));
      }

      progress.append(format("\tSubtask Retry rate: %.2f%%%n", mRetrySubTasksPercentage));
      progress.append(
          format("\tSubtasks on Retry Dead Letter Queue: %s%n", mRetryDeadLetterQueueSize));
      if (mFailedFileSavedPath != null) {
        progress.append(format("\tFailed files saved to: %s%n", mFailedFileSavedPath));
      }
      return progress.toString();
    }

    private String getJsonReport() {
      try {
        return new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new InternalRuntimeException("Failed to convert LoadProgressReport to JSON", e);
      }
    }
  }

  // metrics
  enum FailureReason {
    CANCELLED,
    INTERRUPTED,
    MEMBERSHIP_CHANGED,
    WORKER_FAILED,
    WORKER_RPC_FAILED,
    WORKER_NOT_REACHABLE;
  }

  public static final Counter JOB_LOAD_SUCCESS =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_SUCCESS.getName());
  public static final Counter JOB_LOAD_FAIL =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FAIL.getName());

  public static final Counter LOAD_FILE_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_INODES_COUNT.getName());

  public static final Counter LOAD_FAIL_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FAILURE_COUNT.getName());

  public static final Counter LOAD_FILE_SIZE =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FILE_SIZE.getName());
  public static final Meter LOAD_RATE =
      MetricsSystem.meter(MetricKey.MASTER_JOB_LOAD_RATE.getName());
}

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

package alluxio.master.table.transform;

import alluxio.client.job.JobMasterClient;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobConfig;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.table.AlluxioCatalog;
import alluxio.master.table.Partition;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Table.AddTransformJobInfoEntry;
import alluxio.proto.journal.Table.RemoveTransformJobInfoEntry;
import alluxio.resource.CloseableIterator;
import alluxio.security.user.UserState;
import alluxio.table.common.Layout;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages transformations.
 *
 * It executes a transformation by submitting a job to the job service.
 *
 * A background thread periodically checks whether the job succeeds, fails, or is still running,
 * the period is configurable by {@link PropertyKey#TABLE_TRANSFORM_MANAGER_JOB_MONITOR_INTERVAL}.
 *
 * It keeps information about all running transformations not only in memory,
 * but also in the journal,so even if it is restarted, the information of the previous running
 * transformations is not lost.
 *
 * It keeps history of all succeeded or failed transformations in memory for a time period which is
 * configurable by {@link PropertyKey#TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME}.
 *
 * If the job succeeds, it updates the {@link Partition}'s location by
 * {@link AlluxioCatalog#completeTransformTable(JournalContext, String, String, String, Map)}.
 */
public class TransformManager implements DelegatingJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(TransformManager.class);
  private static final long INVALID_JOB_ID = -1;

  /** The function used to create a {@link JournalContext}. */
  private final ThrowingSupplier<JournalContext, UnavailableException> mCreateJournalContext;

  /** The catalog. */
  private final AlluxioCatalog mCatalog;

  /** The client to talk to job master. */
  private final JobMasterClient mJobMasterClient;

  /**
   * A cache from job ID to the job's information.
   * It contains history of finished jobs, no matter whether it succeeds or fails.
   * Each history is kept for a configurable time period.
   * This is not journaled, so after restarting TableMaster, the job history is lost.
   */
  private final Cache<Long, TransformJobInfo> mJobHistory = CacheBuilder.newBuilder()
      .expireAfterWrite(ServerConfiguration.getMs(
          PropertyKey.TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME),
          TimeUnit.MILLISECONDS)
      .build();

  /** Journaled state. */
  private final State mState = new State();

  /**
   * An internal job master client will be created.
   *
   * @param createJournalContext journal context creator
   * @param catalog the table catalog
   * @param jobMasterClient the job master client
   */
  public TransformManager(
      ThrowingSupplier<JournalContext, UnavailableException> createJournalContext,
      AlluxioCatalog catalog, JobMasterClient jobMasterClient) {
    mCreateJournalContext = createJournalContext;
    mCatalog = catalog;
    mJobMasterClient = jobMasterClient;
  }

  /**
   * Starts background heartbeats.
   * The heartbeats are stopped when unchecked error happens inside the heartbeats,
   * or the hearbeat threads are interrupted,
   * or the provided executor service is stopped.
   * This class will not stop the executor service.
   *
   * @param executorService the executor service for executing heartbeat threads
   * @param userState the user state for the heartbeat
   */
  public void start(ExecutorService executorService, UserState userState) {
    executorService.submit(
        new HeartbeatThread(HeartbeatContext.MASTER_TABLE_TRANSFORMATION_MONITOR, new JobMonitor(),
            ServerConfiguration.getMs(PropertyKey.TABLE_TRANSFORM_MANAGER_JOB_MONITOR_INTERVAL),
            ServerConfiguration.global(), userState));
  }

  /**
   * Executes the plans for the table transformation.
   *
   * This method executes a transformation job with type{@link CompositeConfig},
   * the transformation job concurrently executes the plans,
   * each plan has a list of jobs to be executed sequentially.
   *
   * This method triggers the execution of the transformation job asynchronously without waiting
   * for it to finish. The returned job ID can be used to poll the job service for the status of
   * this transformation.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param definition the parsed transformation definition
   * @return the job ID for the transformation job
   * @throws IOException when there is an ongoing transformation on the table, or the transformation
   *    job fails to be started, or all partitions of the table have been transformed with the same
   *    definition
   */
  public long execute(String dbName, String tableName, TransformDefinition definition)
      throws IOException {
    List<TransformPlan> plans = mCatalog.getTransformPlan(dbName, tableName, definition);
    if (plans.isEmpty()) {
      throw new IOException(ExceptionMessage.TABLE_ALREADY_TRANSFORMED.getMessage(
          dbName, tableName, definition.getDefinition()));
    }
    Pair<String, String> dbTable = new Pair<>(dbName, tableName);
    // Atomically try to acquire the permit to execute the transformation job.
    // This PUT does not need to be journaled, because if this PUT succeeds and master crashes,
    // when master restarts, this temporary placeholder entry will not exist, which is correct
    // behavior.
    Long existingJobId = mState.acquireJobPermit(dbTable);
    if (existingJobId != null) {
      if (existingJobId == INVALID_JOB_ID) {
        throw new IOException("A concurrent transformation request is going to be executed");
      } else {
        throw new IOException(ExceptionMessage.TABLE_BEING_TRANSFORMED
            .getMessage(existingJobId.toString(), tableName, dbName));
      }
    }

    ArrayList<JobConfig> concurrentJobs = new ArrayList<>(plans.size());
    for (TransformPlan plan : plans) {
      concurrentJobs.add(new CompositeConfig(plan.getJobConfigs(), true));
    }
    CompositeConfig transformJob = new CompositeConfig(concurrentJobs, false);

    long jobId;
    try {
      jobId = mJobMasterClient.run(transformJob);
    } catch (IOException e) {
      // The job fails to start, clear the acquired permit for execution.
      // No need to journal this REMOVE, if master crashes, when it restarts, the permit placeholder
      // entry will not exist any more, which is correct behavior.
      mState.releaseJobPermit(dbTable);
      String error = String.format("Fails to start job to transform table %s in database %s",
          tableName, dbName);
      LOG.error(error, e);
      throw new IOException(error, e);
    }

    Map<String, Layout> transformedLayouts = new HashMap<>(plans.size());
    for (TransformPlan plan : plans) {
      transformedLayouts.put(plan.getBaseLayout().getSpec(), plan.getTransformedLayout());
    }
    AddTransformJobInfoEntry journalEntry = AddTransformJobInfoEntry.newBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setDefinition(definition.getDefinition())
        .setJobId(jobId)
        .putAllTransformedLayouts(Maps.transformValues(transformedLayouts, Layout::toProto))
        .build();
    try (JournalContext journalContext = mCreateJournalContext.apply()) {
      applyAndJournal(journalContext, Journal.JournalEntry.newBuilder()
          .setAddTransformJobInfo(journalEntry).build());
    }
    return jobId;
  }

  /**
   * @param jobId the job ID
   * @return the job information
   */
  public Optional<TransformJobInfo> getTransformJobInfo(long jobId) {
    TransformJobInfo job = mState.getRunningJob(jobId);
    if (job == null) {
      job = mJobHistory.getIfPresent(jobId);
    }
    return job == null ? Optional.empty() : Optional.of(job);
  }

  /**
   * @return all transformation jobs, including the running jobs and the kept finished jobs,
   *    sorted by job ID in increasing order
   */
  public List<TransformJobInfo> getAllTransformJobInfo() {
    List<TransformJobInfo> jobs = Lists.newArrayList(mJobHistory.asMap().values());
    jobs.addAll(mState.getRunningJobs());
    jobs.sort(Comparator.comparing(TransformJobInfo::getJobId));
    return jobs;
  }

  @Override
  public Journaled getDelegate() {
    return mState;
  }

  /**
   * Periodically polls the job service to monitor the status of the running transformation jobs,
   * if a transformation job succeeds, then update the Table partitions' layouts.
   */
  private final class JobMonitor implements HeartbeatExecutor {
    private void onFinish(TransformJobInfo job) {
      mJobHistory.put(job.getJobId(), job);
      RemoveTransformJobInfoEntry journalEntry = RemoveTransformJobInfoEntry.newBuilder()
          .setDbName(job.getDb())
          .setTableName(job.getTable())
          .build();
      try (JournalContext journalContext = mCreateJournalContext.apply()) {
        applyAndJournal(journalContext, Journal.JournalEntry.newBuilder()
            .setRemoveTransformJobInfo(journalEntry).build());
      } catch (UnavailableException e) {
        LOG.error("Failed to create journal for RemoveTransformJobInfo for database {} table {}",
            job.getDb(), job.getTable());
      }
    }

    /**
     * Handle the cases where a job fails, is cancelled, or job ID not found.
     *
     * @param job the transformation job
     * @param status the job status
     * @param error the job error message
     */
    private void handleJobError(TransformJobInfo job, Status status, String error) {
      job.setJobStatus(status);
      job.setJobErrorMessage(error);
      onFinish(job);
    }

    /**
     * Handle the case where a job is completed.
     *
     * @param job the transformation job
     */
    private void handleJobSuccess(TransformJobInfo job) {
      try (JournalContext journalContext = mCreateJournalContext.apply()) {
        mCatalog.completeTransformTable(journalContext, job.getDb(), job.getTable(),
            job.getDefinition(), job.getTransformedLayouts());
        job.setJobStatus(Status.COMPLETED);
      } catch (IOException e) {
        String error = String.format("Failed to update partition layouts for database %s table %s",
            job.getDb(), job.getTable());
        LOG.error(error);
        job.setJobStatus(Status.FAILED);
        job.setJobErrorMessage(error);
      }
      onFinish(job);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      for (TransformJobInfo job : mState.getRunningJobs()) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException(ExceptionMessage.TRANSFORM_MANAGER_HEARTBEAT_INTERRUPTED
              .getMessage());
        }
        long jobId = job.getJobId();
        try {
          LOG.debug("Polling for status of transformation job {}", jobId);
          JobInfo jobInfo = mJobMasterClient.getJobStatus(jobId);
          switch (jobInfo.getStatus()) {
            case FAILED: // fall through
            case CANCELED: // fall through
              LOG.warn("Transformation job {} for database {} table {} {}: {}", jobId,
                  job.getDb(), job.getTable(),
                  jobInfo.getStatus() == Status.FAILED ? "failed" : "canceled",
                  jobInfo.getErrorMessage());
              handleJobError(job, jobInfo.getStatus(), jobInfo.getErrorMessage());
              break;
            case COMPLETED:
              LOG.info("Transformation job {} for database {} table {} succeeds", jobId,
                  job.getDb(), job.getTable());
              handleJobSuccess(job);
              break;
            case RUNNING: // fall through
            case CREATED:
              break;
            default:
              throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
          }
        } catch (NotFoundException e) {
          String error = ExceptionMessage.TRANSFORM_JOB_ID_NOT_FOUND_IN_JOB_SERVICE.getMessage(
              jobId, job.getDb(), job.getTable(), e.getMessage());
          LOG.warn(error);
          handleJobError(job, Status.FAILED, error);
        } catch (IOException e) {
          LOG.error("Failed to get status for job (id={})", jobId, e);
        }
      }
    }

    @Override
    public void close() {
      // EMPTY
    }
  }

  /**
   * Journaled state.
   *
   * The internal data structure should never be exposed outside of the class,
   * all changes to the internal state should happen through applying journal entries.
   */
  private final class State implements Journaled {
    /**
     * Map from (db, table) to the ID of the running transformation job.
     * When trying to start a transformation on a table, a placeholder ID is put first.
     * When removing a job, first remove from {@link #mRunningJobs}, then remove from this map,
     * otherwise, there might be concurrent transformations running on the same table.
     */
    private final ConcurrentHashMap<Pair<String, String>, Long> mRunningJobIds =
        new ConcurrentHashMap<>();
    /**
     * Map from job ID to job info.
     */
    private final ConcurrentHashMap<Long, TransformJobInfo> mRunningJobs =
        new ConcurrentHashMap<>();

    /**
     * @return all running jobs
     */
    public Collection<TransformJobInfo> getRunningJobs() {
      return mRunningJobs.values();
    }

    /**
     * @param jobId the job ID
     * @return corresponding job or null if not exists
     */
    public TransformJobInfo getRunningJob(long jobId) {
      return mRunningJobs.get(jobId);
    }

    /**
     * Acquires a permit for transforming a table.
     *
     * @param dbTable a pair of database and table name
     * @return the ID of the existing transformation on the table, or null
     */
    public Long acquireJobPermit(Pair<String, String> dbTable) {
      return mRunningJobIds.putIfAbsent(dbTable, INVALID_JOB_ID);
    }

    /**
     * Releases the previously acquired permit for transforming a table.
     *
     * @param dbTable a pair of database and table name
     */
    public void releaseJobPermit(Pair<String, String> dbTable) {
      mRunningJobIds.remove(dbTable);
    }

    @Override
    public boolean processJournalEntry(JournalEntry entry) {
      if (entry.hasAddTransformJobInfo()) {
        applyAddTransformJobInfoEntry(entry.getAddTransformJobInfo());
      } else if (entry.hasRemoveTransformJobInfo()) {
        applyRemoveTransformJobInfoEntry(entry.getRemoveTransformJobInfo());
      } else {
        return false;
      }
      return true;
    }

    private void applyAddTransformJobInfoEntry(AddTransformJobInfoEntry entry) {
      Map<String, alluxio.grpc.table.Layout> layouts = entry.getTransformedLayoutsMap();
      Map<String, Layout> transformedLayouts = Maps.transformValues(layouts,
          layout -> mCatalog.getLayoutRegistry().create(layout));
      TransformJobInfo job = new TransformJobInfo(entry.getDbName(), entry.getTableName(),
          entry.getDefinition(), entry.getJobId(), transformedLayouts);
      mRunningJobIds.put(job.getDbTable(), job.getJobId());
      mRunningJobs.put(job.getJobId(), job);
    }

    private void applyRemoveTransformJobInfoEntry(RemoveTransformJobInfoEntry entry) {
      Pair<String, String> dbTable = new Pair<>(entry.getDbName(), entry.getTableName());
      long jobId = mRunningJobIds.get(dbTable);
      mRunningJobs.remove(jobId);
      mRunningJobIds.remove(dbTable);
    }

    @Override
    public void resetState() {
      mRunningJobs.clear();
      mRunningJobIds.clear();
      mJobHistory.invalidateAll();
      mJobHistory.cleanUp();
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(
          Iterators.transform(mRunningJobs.values().iterator(), job -> {
            AddTransformJobInfoEntry journal = AddTransformJobInfoEntry.newBuilder()
                .setDbName(job.getDb())
                .setTableName(job.getTable())
                .setDefinition(job.getDefinition())
                .setJobId(job.getJobId())
                .putAllTransformedLayouts(Maps.transformValues(
                    job.getTransformedLayouts(), Layout::toProto))
                .build();
            return JournalEntry.newBuilder().setAddTransformJobInfo(journal).build();
          }));
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.TABLE_MASTER_TRANSFORM_MANAGER;
    }
  }

  /**
   * A supplier with return type R that might throw exception E.
   *
   * @param <R> the return type
   * @param <E> the exception type
   */
  @FunctionalInterface
  public interface ThrowingSupplier<R, E extends Throwable> {
    /**
     * @return the result
     */
    R apply() throws E;
  }
}

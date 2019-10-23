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

import alluxio.ClientContext;
import alluxio.client.job.JobMasterClient;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobConfig;
import alluxio.job.composite.CompositeConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.table.AlluxioCatalog;
import alluxio.master.table.Partition;
import alluxio.master.table.Table;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Table.RemoveTransformJobEntry;
import alluxio.proto.journal.Table.TransformJobEntry;
import alluxio.security.user.UserState;
import alluxio.table.common.Layout;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Manages transformations.
 */
public class TransformManager implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(TransformManager.class);
  private static final long INVALID_JOB_ID = -1;
  private static final TransformJob EMPTY_TRANSFORM_JOB =
      new TransformJob("", INVALID_JOB_ID, Collections.emptyMap());

  /**
   * Information kept for a transformation job.
   */
  private static final class TransformJob {
    private final String mDefinition;
    private final long mJobId;
    private final Map<String, Layout> mTransformedLayouts;

    /**
     * @param definition the transformation definition
     * @param jobId the job ID
     * @param transformedLayouts the mapping from a partition spec to its transformed layout
     */
    TransformJob(String definition, long jobId, Map<String, Layout> transformedLayouts) {
      mDefinition = definition;
      mJobId = jobId;
      mTransformedLayouts = Collections.unmodifiableMap(transformedLayouts);
    }

    /**
     * @return the transformation definition
     */
    String getDefinition() {
      return mDefinition;
    }

    /**
     * @return the job ID
     */
    long getJobId() {
      return mJobId;
    }

    /**
     * @return a read-only mapping from a partition spec to its transformed layout
     */
    Map<String, Layout> getTransformedLayouts() {
      return mTransformedLayouts;
    }
  }

  private final ThrowingSupplier<JournalContext, UnavailableException> mCreateJournalContext;
  private final AlluxioCatalog mCatalog;
  /**
   * The client to talk to job master.
   */
  private final JobMasterClient mJobMasterClient;
  /**
   * A map from (database, table) to the ID of the transformation job.
   * This will be journaled.
   */
  private final Map<Pair<String, String>, TransformJob> mJobs = new ConcurrentHashMap<>();

  /**
   * An internal job master client will be created.
   *
   * @param createJournalContext journal context creator
   * @param catalog the table catalog
   */
  public TransformManager(
      ThrowingSupplier<JournalContext, UnavailableException> createJournalContext,
      AlluxioCatalog catalog) {
    mCreateJournalContext = createJournalContext;
    mCatalog = catalog;
    mJobMasterClient = JobMasterClient.Factory.create(JobMasterClientContext.newBuilder(
        ClientContext.create(ServerConfiguration.global())).build());
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
    List<TransformPlan> plans = mCatalog.getTable(dbName, tableName)
        .getTransformPlans(definition);
    if (plans.isEmpty()) {
      throw new IOException(String.format("Database %s table %s has already been transformed by "
          + "definition '%s'", dbName, tableName, definition.getDefinition()));
    }
    Pair<String, String> table = new Pair<>(dbName, tableName);
    // Atomically try to acquire the permit to execute the transformation job.
    // This PUT does not need to be journaled, because if this PUT succeeds and master crashes,
    // when master restarts, this temporary placeholder entry will not exist, which is correct
    // behavior.
    TransformJob existingJob = mJobs.putIfAbsent(table, EMPTY_TRANSFORM_JOB);
    if (existingJob != null) {
      if (existingJob.getJobId() == INVALID_JOB_ID) {
        throw new IOException("A concurrent transformation request is going to be executed");
      } else {
        throw new IOException(
            String.format("Existing job (%d) is transforming table (%s) in database (%s)",
            existingJob.getJobId(), tableName, dbName));
      }
    }

    ArrayList<JobConfig> concurrentJobs = new ArrayList<>(plans.size());
    for (TransformPlan plan : plans) {
      concurrentJobs.add(new CompositeConfig(plan.getJobConfigs(), true));
    }
    CompositeConfig transformJob = new CompositeConfig(concurrentJobs, false);

    long jobId = INVALID_JOB_ID;
    try {
      jobId = mJobMasterClient.run(transformJob);
    } catch (IOException e) {
      LOG.warn("Job (id={}) fails to start to transform table {} in database {}",
          jobId, tableName, dbName);
      // The job fails to start, clear the acquired permit for execution.
      // No need to journal this REMOVE, if master crashes, when it restarts, the permit placeholder
      // entry will not exist any more, which is correct behavior.
      mJobs.remove(table);
      return INVALID_JOB_ID;
    }

    Map<String, Layout> transformedLayouts = new HashMap<>(plans.size());
    for (TransformPlan plan : plans) {
      transformedLayouts.put(plan.getBaseLayout().getSpec(), plan.getTransformedLayout());
    }
    mJobs.put(table, new TransformJob(definition.getDefinition(), jobId, transformedLayouts));
    TransformJobEntry journalEntry = TransformJobEntry.newBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setDefinition(definition.getDefinition())
        .setJobId(jobId)
        .putAllTransformedLayouts(Maps.transformValues(transformedLayouts, Layout::toProto))
        .build();
    try (JournalContext journalContext = mCreateJournalContext.apply()) {
      applyAndJournal(journalContext, Journal.JournalEntry.newBuilder()
          .setTransformJob(journalEntry).build());
    }
    return jobId;
  }

  /**
   * Periodically polls the job service to monitor the status of the running transformation jobs,
   * if a transformation job succeeds, then update the Table partitions' layouts.
   */
  private final class JobMonitor implements HeartbeatExecutor {
    private void completePartitionTransformation(String dbName, String tableName, TransformJob job)
        throws IOException {
      Table table = mCatalog.getTable(dbName, tableName);
      for (Map.Entry<String, Layout> entry : job.getTransformedLayouts().entrySet()) {
        String spec = entry.getKey();
        Layout layout = entry.getValue();
        Partition partition = table.getPartition(spec);
        partition.transform(job.getDefinition(), layout);
        LOG.debug("Transformed partition {} to {} with definition {}", spec, layout,
            job.getDefinition());
      }
    }

    private void removeTransformationJobForTable(Pair<String, String> table) {
      mJobs.remove(table);
      RemoveTransformJobEntry journalEntry = RemoveTransformJobEntry.newBuilder()
          .setDbName(table.getFirst())
          .setTableName(table.getSecond())
          .build();
      try (JournalContext journalContext = mCreateJournalContext.apply()) {
        applyAndJournal(journalContext, Journal.JournalEntry.newBuilder()
            .setRemoveTransformJob(journalEntry).build());
      } catch (UnavailableException e) {
        LOG.error("Failed to create journal for RemoveTransformJob for database {} table {}",
            table.getFirst(), table.getSecond());
      }
    }

    /**
     * Handle the cases where a job fails, is cancelled, or job ID not found.
     *
     * @param table the (database, table) pair
     */
    private void handleJobError(Pair<String, String> table) {
      removeTransformationJobForTable(table);
    }

    /**
     * Handle the case where a job is completed.
     *
     * @param table the (database, table) pair
     * @param job the transformation job
     */
    private void handleJobSuccess(Pair<String, String> table, TransformJob job) {
      try {
        completePartitionTransformation(table.getFirst(), table.getSecond(), job);
      } catch (IOException e) {
        LOG.error("Failed to update partition layouts for database {} table {}",
            table.getFirst(), table.getSecond());
      }
      removeTransformationJobForTable(table);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      for (Map.Entry<Pair<String, String>, TransformJob> entry : mJobs.entrySet()) {
        if (Thread.interrupted()) {
          throw new InterruptedException("JobMonitor interrupted.");
        }
        Pair<String, String> table = entry.getKey();
        TransformJob job = entry.getValue();
        long jobId = entry.getValue().getJobId();
        if (jobId == INVALID_JOB_ID) {
          continue;
        }
        try (JobMasterClient client = JobMasterClient.Factory.create(JobMasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build())) {
          LOG.debug("Polling for status of transformation job {}", jobId);
          JobInfo jobInfo = client.getStatus(jobId);
          switch (jobInfo.getStatus()) {
            case FAILED: // fall through
            case CANCELED: // fall through
              LOG.warn("Transformation job {} for database {} table {} {}: {}", jobId,
                  table.getFirst(), table.getSecond(),
                  jobInfo.getStatus() == Status.FAILED ? "failed" : "canceled",
                  jobInfo.getErrorMessage());
              handleJobError(table);
              break;
            case COMPLETED:
              String dbName = table.getFirst();
              String tableName = table.getSecond();
              LOG.info("Transformation job {} for database {} table {} succeeds", jobId,
                  dbName, tableName);
              handleJobSuccess(table, job);
              break;
            case RUNNING: // fall through
            case CREATED:
              break;
            default:
              throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
          }
        } catch (NotFoundException e) {
          LOG.warn("Transformation job {} for database {} table {} no longer exists", jobId,
              table.getFirst(), table.getSecond());
          handleJobError(table);
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

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    if (entry.hasTransformJob()) {
      applyTransformJobEntry(entry.getTransformJob());
    } else if (entry.hasRemoveTransformJob()) {
      applyRemoveTransformJobEntry(entry.getRemoveTransformJob());
    } else {
      return false;
    }
    return true;
  }

  private void applyTransformJobEntry(TransformJobEntry entry) {
    Pair<String, String> table = new Pair<>(entry.getDbName(), entry.getTableName());
    Map<String, alluxio.grpc.table.Layout> layouts = entry.getTransformedLayoutsMap();
    Map<String, Layout> transformedLayouts = Maps.transformValues(layouts,
        layout -> mCatalog.getLayoutRegistry().create(layout));
    TransformJob job = new TransformJob(entry.getDefinition(), entry.getJobId(),
        transformedLayouts);
    mJobs.put(table, job);
  }

  private void applyRemoveTransformJobEntry(RemoveTransformJobEntry entry) {
    mJobs.remove(new Pair<>(entry.getDbName(), entry.getTableName()));
  }

  @Override
  public void resetState() {
    mJobs.clear();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.transform(mJobs.entrySet().iterator(), entry -> {
      String dbName = entry.getKey().getFirst();
      String tableName = entry.getKey().getSecond();
      TransformJob job = entry.getValue();
      TransformJobEntry journal = TransformJobEntry.newBuilder()
          .setDbName(dbName)
          .setTableName(tableName)
          .setDefinition(job.getDefinition())
          .setJobId(job.getJobId())
          .putAllTransformedLayouts(Maps.transformValues(
              job.getTransformedLayouts(), Layout::toProto))
          .build();
      return JournalEntry.newBuilder().setTransformJob(journal).build();
    });
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.TABLE_MASTER_TRANSFORM_MANAGER;
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

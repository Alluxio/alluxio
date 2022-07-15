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

package alluxio.master.file.loadmanager;

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundRuntimeException;
import alluxio.exception.status.ResourceExhaustedRuntimeException;
import alluxio.exception.status.UnauthenticatedRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CheckAccessContext;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Load manager which controls load operations.
 */
public final class LoadManager {
  private static final Logger LOG = LoggerFactory.getLogger(LoadManager.class);
  private static final int CAPACITY = 100;
  private static final int BATCH_SIZE = 50;
  private static final long WORKER_UPDATE_INTERVAL = Configuration.getMs(
      PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME);

  private final FileSystemMaster mFileSystemMaster;
  private final FileSystemContext mContext;
  private final Map<String, LoadJob> mLoadJobs = new ConcurrentHashMap<>();
  private final Map<LoadJob, Set<WorkerInfo>> mLoadTasks = new ConcurrentHashMap<>();
  private final ScheduledExecutorService mLoadScheduler =
      Executors.newSingleThreadScheduledExecutor();
  private Map<WorkerInfo, CloseableResource<BlockWorkerClient>> mActiveWorkers = new HashMap<>();

  /**
   * Constructor.
   * @param fileSystemMaster fileSystemMaster
   * @param context fileSystemContext
   */
  public LoadManager(FileSystemMaster fileSystemMaster, FileSystemContext context) {
    mFileSystemMaster = fileSystemMaster;
    mContext = context;
  }

  /**
   * Start load manager.
   */
  void start() {
    mLoadScheduler.scheduleAtFixedRate(this::updateWorkers,
        0, WORKER_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
    mLoadScheduler.scheduleAtFixedRate(this::processJobs, 0, 1, TimeUnit.SECONDS);
  }

  /**
   * Stop load manager.
   */
  public void stop() {
    mLoadScheduler.shutdown();
    mActiveWorkers.values().forEach(CloseableResource::close);
  }

  /**
   * Submit a load job.
   * @param loadPath alluxio directory path to load into Alluxio
   * @param bandwidth bandwidth allocated to this load
   * @return boolean true is the job is new, false if the job has already been submitted
   */
  public boolean submit(String loadPath, int bandwidth) {
    try {
      mFileSystemMaster.checkAccess(new AlluxioURI(loadPath), CheckAccessContext.defaults());
    } catch (FileDoesNotExistException | InvalidPathException e) {
      throw new NotFoundRuntimeException(e);
    } catch (AccessControlException e) {
      throw new UnauthenticatedRuntimeException(e);
    } catch (IOException e) {
      throw AlluxioRuntimeException.fromIOException(e);
    }
    return submit(new LoadJob(loadPath, bandwidth));
  }

  /**
   * Submit a load job.
   * @param load the load job
   * @return boolean true is the job is new, false if the job has already been submitted
   */
  @VisibleForTesting
  public boolean submit(LoadJob load) {
    if (mLoadJobs.containsKey(load.getPath()) && !mLoadJobs.get(load.getPath()).isDone()) {
      mLoadJobs.get(load.getPath()).updateBandwidth(load.getBandWidth());
      return false;
    }
    if (mLoadTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException(
          "Too many load jobs running, please submit later.");
    }

    mLoadJobs.put(load.getPath(), load);
    mLoadTasks.put(load, new HashSet<>());
    return true;
  }

  /**
   * Get active workers.
   * @return active workers
   */
  @VisibleForTesting
  public Map<WorkerInfo, CloseableResource<BlockWorkerClient>> getActiveWorkers() {
    return mActiveWorkers;
  }

  /**
   * Refresh active workers.
   */
  @VisibleForTesting
  public void updateWorkers() {
    Set<WorkerInfo> workerInfos;
    try {
      workerInfos = ImmutableSet.copyOf(mFileSystemMaster.getWorkerInfoList());
    } catch (UnavailableException e) {
      LOG.warn("Failed to get worker info, using existing worker infos of {} workers",
          mActiveWorkers.size());
      return;
    }
    if (workerInfos.size() == mActiveWorkers.size()
        && workerInfos.containsAll(mActiveWorkers.keySet())) {
      return;
    }

    Map<WorkerInfo, CloseableResource<BlockWorkerClient>> activeWorkers = new HashMap<>();
    for (WorkerInfo workerInfo : workerInfos) {
      if (mActiveWorkers.containsKey(workerInfo)) {
        activeWorkers.put(workerInfo, mActiveWorkers.get(workerInfo));
      } else {
        try {
          activeWorkers.put(
              workerInfo, mContext.acquireBlockWorkerClient(workerInfo.getAddress()));
        } catch (IOException e) {
          // skip the worker if we cannot obtain a client
        }
      }
    }
    mActiveWorkers = activeWorkers;
  }

  /**
   * Get load jobs.
   * @return load jobs
   */
  @VisibleForTesting
  public Map<String, LoadJob> getLoadJobs() {
    return mLoadJobs;
  }

  private void processJobs() {
    mLoadTasks.forEach(this::processJob);
  }

  private void processJob(LoadJob load, Set<WorkerInfo> loadWorkers) {
    switch (load.getStatus()) {
      case SUBMITTED:
        Preconditions.checkState(
            loadWorkers.size() == 0,
            String.format("New job should not have outstanding requests, but load %s has %d",
                load.getPath(), mLoadTasks.get(load).size()));
        mActiveWorkers.forEach((workerInfo, workerClient) -> {
          if (scheduleBatch(load, workerInfo, loadWorkers, workerClient, BATCH_SIZE)) {
            loadWorkers.add(workerInfo);
          }
        });
        load.setStatus(LoadJob.LoadStatus.PROCESSING);
        break;
      case PROCESSING:
        // Check job health
        if (!load.isHealthy()) {
          load.setStatus(LoadJob.LoadStatus.FAILED);
          break;
        }
        Map<WorkerInfo, CloseableResource<BlockWorkerClient>> newWorkers =
            mActiveWorkers.entrySet().stream()
                .filter(entry -> !loadWorkers.contains(entry.getKey()))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        // If there are new workers, schedule job onto new workers
        newWorkers.forEach((workerInfo, workerClient) -> {
          if (scheduleBatch(load, workerInfo, loadWorkers, workerClient, BATCH_SIZE)) {
            loadWorkers.add(workerInfo);
          }
        });
        if (loadWorkers.isEmpty()) {
          load.setStatus(LoadJob.LoadStatus.VERIFYING);
        }
        break;
      case VERIFYING:
        // TODO(rongrong)
        load.setStatus(LoadJob.LoadStatus.SUCCEEDED);
        break;
      case FAILED:
      case SUCCEEDED:
        mLoadTasks.remove(load);
        break;
      default:
        throw new IllegalStateException(String.format("Unhandled status: %s", load.getStatus()));
    }
  }

  // Returns false if the whole task fails
  private boolean processTask(
      LoadJob load, LoadRequest request,
      ListenableFuture<LoadResponse> responseFuture) {
    try {
      LoadResponse response = responseFuture.get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        for (BlockStatus status : response.getBlockStatusList()) {
          if (!status.getRetryable() || !load.addBlockToRetry(status.getBlock())) {
            load.addBlockFailure(
                status.getBlock(), Status.fromCodeValue(status.getCode()));
          }
        }
      }
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (CancellationException | ExecutionException e) {
      request.getBlocksList().forEach(load::addBlockToRetry);
      return false;
    }
    catch (InterruptedException e) {
      request.getBlocksList().forEach(load::addBlockToRetry);
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  private boolean scheduleBatch(
      LoadJob load,
      WorkerInfo workerInfo,
      Set<WorkerInfo> loadWorkers,
      CloseableResource<BlockWorkerClient> workerClient,
      int batchSize) {
    List<Block> batch = load.getNextBatch(mFileSystemMaster, batchSize);
    if (batch.isEmpty()) {
      return false;
    }
    LoadRequest request = buildRequest(
        batch, load.getBandWidth() / mActiveWorkers.size());
    ListenableFuture<LoadResponse> responseFuture = workerClient.get().load(request);
    responseFuture.addListener(() -> {
      if (!processTask(load, request, responseFuture)) {
        mActiveWorkers.remove(workerInfo);
      }
      // Schedule new work
      if (load.isHealthy()) {
        if (mActiveWorkers.containsKey(workerInfo)) {
          if (!scheduleBatch(
              load, workerInfo, loadWorkers, mActiveWorkers.get(workerInfo), BATCH_SIZE)) {
            loadWorkers.remove(workerInfo);
          }
        } else {
          loadWorkers.remove(workerInfo);
        }
      }
    }, mLoadScheduler);

    return true;
  }

  private static LoadRequest buildRequest(List<Block> blockBatch, int bandwidth) {
    return LoadRequest
        .newBuilder()
        .setBandwidth(bandwidth)
        .addAllBlocks(blockBatch)
        .build();
  }
}

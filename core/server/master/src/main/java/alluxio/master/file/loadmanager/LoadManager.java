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
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
      Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("load-manager-scheduler", false));
  private Map<WorkerInfo, CloseableResource<BlockWorkerClient>> mActiveWorkers = ImmutableMap.of();

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
   * @param verificationEnabled whether to run verification step or not
   * @return true if the job is new, false if the job has already been submitted
   */
  public boolean submitLoad(String loadPath, int bandwidth, boolean verificationEnabled) {
    try {
      mFileSystemMaster.checkAccess(new AlluxioURI(loadPath), CheckAccessContext.defaults());
    } catch (FileDoesNotExistException | InvalidPathException e) {
      throw new NotFoundRuntimeException(e);
    } catch (AccessControlException e) {
      throw new UnauthenticatedRuntimeException(e);
    } catch (IOException e) {
      throw AlluxioRuntimeException.fromIOException(e);
    }
    return submitLoad(new LoadJob(loadPath, bandwidth, verificationEnabled));
  }

  /**
   * Submit a load job.
   * @param loadJob the load job
   * @return true if the job is new, false if the job has already been submitted
   */
  @VisibleForTesting
  public boolean submitLoad(LoadJob loadJob) {
    LoadJob existingJob = mLoadJobs.get(loadJob.getPath());
    if (existingJob != null && !existingJob.isDone()) {
      existingJob.updateBandwidth(loadJob.getBandWidth());
      existingJob.setVerificationEnabled(loadJob.isVerificationEnabled());
      // If there's a stopped job, re-enable it, save some work for already loaded blocks
      if (existingJob.getStatus() == LoadJob.LoadStatus.STOPPED) {
        existingJob.setStatus(LoadJob.LoadStatus.LOADING);
      }
      return false;
    }

    if (mLoadTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException(
          "Too many load jobs running, please submit later.");
    }

    mLoadJobs.put(loadJob.getPath(), loadJob);
    mLoadTasks.put(loadJob, new HashSet<>());
    return true;
  }

  /**
   * Stop a load job.
   * @param loadPath alluxio directory path to load into Alluxio
   * @return true if the job is stopped, false if the job does not exist or has already finished
   */
  public boolean stopLoad(String loadPath) {
    LoadJob existingJob = mLoadJobs.get(loadPath);
    if (existingJob != null && existingJob.isRunning()) {
      existingJob.setStatus(LoadJob.LoadStatus.STOPPED);
      return true;
    }
    return false;
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

    ImmutableMap.Builder<WorkerInfo, CloseableResource<BlockWorkerClient>> activeWorkers =
        ImmutableMap.builder();
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
    mActiveWorkers = activeWorkers.build();
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

  private void processJob(LoadJob loadJob, Set<WorkerInfo> loadWorkers) {
    if (!loadJob.isRunning()) {
      mLoadTasks.remove(loadJob);
      return;
    }
    if (!loadJob.isHealthy()) {
      loadJob.setStatus(LoadJob.LoadStatus.FAILED);
      return;
    }
    // If there are new workers, schedule job onto new workers
    mActiveWorkers.forEach((workerInfo, workerClient) -> {
      if (!loadWorkers.contains(workerInfo)
          && scheduleBatch(loadJob, workerInfo, loadWorkers, workerClient, BATCH_SIZE)) {
        loadWorkers.add(workerInfo);
      }
    });

    if (loadWorkers.isEmpty() && loadJob.isCurrentLoadDone()) {
      if (loadJob.getCurrentBlockCount() > 0 && loadJob.isVerificationEnabled()) {
        loadJob.initiateVerification();
      } else {
        if (loadJob.isHealthy()) {
          loadJob.setStatus(LoadJob.LoadStatus.SUCCEEDED);
        }
        else {
          loadJob.setStatus(LoadJob.LoadStatus.FAILED);
        }
      }
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
        loadWorkers.remove(workerInfo);
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

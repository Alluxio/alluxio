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

package alluxio.worker.block.management;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.LocalBlockStore;
import alluxio.worker.block.management.tier.TierManagementTaskProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Coordinator for instantiating and running various block management tasks.
 */
public class ManagementTaskCoordinator implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementTaskCoordinator.class);
  /** Duration to sleep when a) load detected on worker. b) no work to do. */
  private static final long LOAD_DETECTION_COOL_DOWN_TIME =
      Configuration.getMs(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME);
  /** The back-off strategy. */
  private static final BackoffStrategy BACKOFF_STRATEGY = Configuration
      .getEnum(PropertyKey.WORKER_MANAGEMENT_BACKOFF_STRATEGY, BackoffStrategy.class);

  /** Runner thread for launching management tasks. */
  private final Thread mRunnerThread;
  /** Executor that will run the management tasks. */
  private final ExecutorService mTaskExecutor = Executors.newFixedThreadPool(
      Configuration.getInt(PropertyKey.WORKER_MANAGEMENT_TASK_THREAD_COUNT),
        ThreadFactoryUtils.build("block-management-task-%d", true));

  private final LocalBlockStore mBlockStore;
  private final BlockMetadataManager mMetadataManager;
  private final StoreLoadTracker mLoadTracker;

  /** This coordinator requires calculating eviction view per each task. */
  private final Supplier<BlockMetadataEvictorView> mEvictionViewSupplier;

  /** List of management task providers. */
  private List<ManagementTaskProvider> mTaskProviders;

  /**
   * Creates management coordinator.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param loadTracker load tracker
   * @param evictionViewSupplier eviction view supplier
   */
  public ManagementTaskCoordinator(LocalBlockStore blockStore, BlockMetadataManager metadataManager,
      StoreLoadTracker loadTracker, Supplier<BlockMetadataEvictorView> evictionViewSupplier) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mLoadTracker = loadTracker;
    mEvictionViewSupplier = evictionViewSupplier;
    initializeTaskProviders();
    // Initialize runner thread.
    mRunnerThread = new Thread(this::runManagement, "block-management-runner");
    mRunnerThread.setDaemon(true);
  }

  /**
   * Starts the coordinator.
   */
  public void start() {
    // Start runner thread.
    mRunnerThread.start();
  }

  @Override
  public void close() throws IOException {
    try {
      // Shutdown task executor.
      mTaskExecutor.shutdownNow();
      // Interrupt and wait for runner thread.
      mRunnerThread.interrupt();
      mRunnerThread.join();
    } catch (Exception e) {
      throw new IOException("Failed to close management task coordinator", e);
    }
  }

  /**
   * Register known task providers by priority order.
   *
   * TODO(ggezer): Re-implement async-cache as {@link BlockManagementTask}.
   */
  private void initializeTaskProviders() {
    mTaskProviders = new ArrayList<>(1);
    if (Configuration.isSet(PropertyKey.WORKER_EVICTOR_CLASS)) {
      LOG.warn("Tier management tasks will be disabled under eviction emulation mode.");
    } else {
      // TODO(ggezer): Improve on views per task type.
      mTaskProviders.add(new TierManagementTaskProvider(mBlockStore, mMetadataManager,
          mEvictionViewSupplier, mLoadTracker, mTaskExecutor));
    }
  }

  /**
   * @return the next management task to run, {@code null} if none pending
   */
  private BlockManagementTask getNextTask() {
    /*
     * Order of providers in the registered list imposes an implicit priority of tasks.
     * As long as a provider gives a task, providers next to it won't be consulted.
     */
    for (ManagementTaskProvider taskProvider : mTaskProviders) {
      BlockManagementTask task = taskProvider.getTask();
      if (task != null) {
        return task;
      }
    }
    // No task provided.
    return null;
  }

  /**
   * Main management loop.
   */
  private void runManagement() {
    while (true) {
      if (Thread.interrupted()) {
        // Coordinator closed.
        LOG.debug("Coordinator interrupted.");
        break;
      }

      BlockManagementTask currentTask;
      try {
        // Back off from worker if configured so.
        if (BACKOFF_STRATEGY == BackoffStrategy.ANY
            && mLoadTracker.loadDetected(BlockStoreLocation.anyTier())) {
          LOG.debug("Load detected. Sleeping {}ms.", LOAD_DETECTION_COOL_DOWN_TIME);
          Thread.sleep(LOAD_DETECTION_COOL_DOWN_TIME);
          continue;
        }

        final BlockManagementTask nextTask = getNextTask();
        if (nextTask == null) {
          LOG.debug("No management task pending. Sleeping {}ms.", LOAD_DETECTION_COOL_DOWN_TIME);
          Thread.sleep(LOAD_DETECTION_COOL_DOWN_TIME);
          continue;
        }

        // Submit and wait for the task.
        currentTask = nextTask;
        LOG.debug("Running task of type:{}", currentTask.getClass().getSimpleName());
        // Run the current task on coordinator thread.
        try {
          BlockManagementTaskResult result = currentTask.run();
          LOG.info("{} finished with result: {}", currentTask.getClass().getSimpleName(), result);

          if (result.noProgress()) {
            LOG.debug("Task made no progress due to failures/back-offs. Sleeping {}ms",
                LOAD_DETECTION_COOL_DOWN_TIME);
            Thread.sleep(LOAD_DETECTION_COOL_DOWN_TIME);
          }
        } catch (Exception e) {
          LOG.error("Management task failed: {}. Error: ", currentTask.getClass().getSimpleName(),
              e);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Throwable t) {
        LOG.error("Unexpected error during block management: ", t);
      }
    }
    LOG.debug("Block management coordinator exited.");
  }
}

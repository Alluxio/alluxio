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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Coordinator for instantiating and running various block management tasks.
 */
public class ManagementTaskCoordinator implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementTaskCoordinator.class);

  /** Duration to sleep when there is no pending task. */
  private final long mIdleSleepMs;
  /** Duration to sleep when load detected on worker. */
  private final long mLoadDetectionCoolDownMs;

  /** Runner thread for launching management tasks. */
  private final Thread mRunnerThread;
  /** Executor that will running the management tasks. */
  private final ExecutorService mTaskExecutor;

  private final BlockStore mBlockStore;
  private final BlockMetadataManager mMetadataManager;
  private final StoreLoadTracker mLoadTracker;

  /** This coordinator requires to calculate eviction view per each task. */
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
  public ManagementTaskCoordinator(BlockStore blockStore, BlockMetadataManager metadataManager,
      StoreLoadTracker loadTracker, Supplier<BlockMetadataEvictorView> evictionViewSupplier) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mLoadTracker = loadTracker;
    mEvictionViewSupplier = evictionViewSupplier;

    initializeTaskProviders();

    // Read configs.
    mLoadDetectionCoolDownMs =
        ServerConfiguration.getMs(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME);
    mIdleSleepMs = ServerConfiguration.getMs(PropertyKey.WORKER_MANAGEMENT_IDLE_SLEEP_TIME);

    // Initialize management task executor.
    // Currently a single management task is active at a time.
    mTaskExecutor = Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("block-management-thread-%d", true));

    // Initialize runner thread.
    mRunnerThread = new Thread(this::runManagement, "block-management-runner");
  }

  /**
   * Starts the coordinator.
   */
  public void start() {
    // Start runner thread.
    mRunnerThread.start();
  }

  /**
   * Register known task providers by priority order.
   *
   * TODO(ggezer): TV2 - Implement pin enforcer as {@link BlockManagementTask}.
   * TODO(ggezer): TV2 - Re-implement async-cache as {@link BlockManagementTask}.
   */
  private void initializeTaskProviders() {
    mTaskProviders = new ArrayList<>(1);
    if (ServerConfiguration.isSet(PropertyKey.WORKER_EVICTOR_CLASS)) {
      LOG.warn("Tier management tasks will be disabled under eviction emulation mode.");
    } else {
      mTaskProviders.add(new TierManagementTaskProvider(mBlockStore, mMetadataManager,
          mEvictionViewSupplier, mLoadTracker));
    }
  }

  /**
   * @return the next management task to run, {@code null} if none pending
   */
  private BlockManagementTask getNextTask() {
    /**
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
        return;
      }

      BlockManagementTask currentTask = null;
      try {
        // Back off if any load detected.
        // TODO(ggezer): TV2 - Trust management tasks' back-off handling.
        if (mLoadTracker.loadDetected(BlockStoreLocation.anyTier())) {
          LOG.info("Load detected.");
          Thread.sleep(mLoadDetectionCoolDownMs);
          continue;
        }

        final BlockManagementTask nextTask = getNextTask();
        if (nextTask == null) {
          LOG.info("No management task pending.");
          Thread.sleep(mIdleSleepMs);
          continue;
        }

        // Submit and wait for the task.
        currentTask = nextTask;
        mTaskExecutor.submit(() -> nextTask.run()).get();
        LOG.info("Management task finished: {}", currentTask.getClass().getSimpleName());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        LOG.error("Management task failed: {}. Error: {}", currentTask.getClass().getSimpleName(),
            e);
      } catch (Throwable t) {
        LOG.error("Unexpected error during block management: {}", t);
      }
    }
    LOG.info("Block management coordinator exited.");
  }

  @Override
  public void close() throws IOException {
    mTaskExecutor.shutdownNow();
    mRunnerThread.interrupt();
  }
}

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

package alluxio.master.file.activesync;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.retry.RetryUtils;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;

import alluxio.util.LogUtils;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically sync the files for a particular mount point.
 */
@NotThreadSafe
public class ActiveSyncer implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncer.class);
  private final FileSystemMaster mFileSystemMaster;
  private final ActiveSyncManager mSyncManager;
  private final MountTable mMountTable;
  private final long mMountId;
  private final Queue<CompletableFuture<?>> mSyncTasks;

  /**
   * Constructs a new {@link ActiveSyncer}.
   *
   * @param fileSystemMaster fileSystem master
   * @param syncManager sync manager
   * @param mountTable mount table
   * @param mountId mount id
   */
  public ActiveSyncer(FileSystemMaster fileSystemMaster, ActiveSyncManager syncManager,
      MountTable mountTable, long mountId) {
    mFileSystemMaster = fileSystemMaster;
    mSyncManager = syncManager;
    mMountId = mountId;
    mMountTable = mountTable;
    mSyncTasks = new LinkedBlockingQueue<>(32);
  }

  @Override
  public void heartbeat() {
    while(mSyncTasks.peek() != null && mSyncTasks.peek().isDone()) {
      mSyncTasks.poll();
    }
    LOG.debug("start Active Syncer heartbeat");

    List<AlluxioURI> filterList =  mSyncManager.getFilterList(mMountId);

    if (filterList == null || filterList.isEmpty()) {
      return;
    }

    try {
      UfsManager.UfsClient ufsclient = Objects.requireNonNull(mMountTable.getUfsClient(mMountId));
      try (CloseableResource<UnderFileSystem> ufsResource = ufsclient.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        if (ufs.supportsActiveSync()) {
          SyncInfo syncInfo = ufs.getActiveSyncInfo();
          // This returns a list of ufsUris that we need to sync.
          Set<AlluxioURI> ufsSyncPoints = syncInfo.getSyncPoints();
          // Parallelize across sync points
          List<CompletableFuture<Long>> tasksPerSync = new ArrayList<>();
          for (AlluxioURI ufsUri : ufsSyncPoints) {
            tasksPerSync.add(CompletableFuture.supplyAsync(() -> {
              processSyncPoint(ufsUri, syncInfo);
              return syncInfo.getTxId();
              }, mSyncManager.getExecutor()));

          }
          // Journal the latest processed txId
          CompletableFuture<Void> syncTask =
              CompletableFuture.allOf(tasksPerSync.toArray(new CompletableFuture<?>[0]))
              .thenRunAsync(() -> mFileSystemMaster
                      .recordActiveSyncTxid(syncInfo.getTxId(), mMountId),
                  mSyncManager.getExecutor());
          while(!mSyncTasks.offer(syncTask)) {
            try {
              CompletableFuture<?> task = mSyncTasks.poll();
              if (task == null) {
                // If the task can't be tracked, then we shouldn't let it run.
                syncTask.cancel(true);
                throw new IllegalStateException("Failed to add sync task to queue, but queue head "
                    + "returned null");
              } else {
                task.get();
              }
            } catch (InterruptedException | ExecutionException e) {
              LogUtils.warnWithException(LOG, "Failed while waiting on task to add new task to "
                  + "head of queue", e);
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("IOException " + Throwables.getStackTraceAsString(e));
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  /**
   * Process a single sync point.
   *
   * @param ufsUri ufs URI for the sync point
   * @param syncInfo active sync info for mount
   */
  private void processSyncPoint(AlluxioURI ufsUri, SyncInfo syncInfo) {
    AlluxioURI alluxioUri = mMountTable.reverseResolve(ufsUri).getUri();
    if (alluxioUri == null) {
      LOG.warn("Unable to reverse resolve ufsUri {}", ufsUri);
      return;
    }
    try {
      if (syncInfo.isForceSync()) {
        LOG.debug("force full sync {}", ufsUri.toString());
        RetryUtils.retry("Full Sync", () -> {
          mFileSystemMaster.activeSyncMetadata(alluxioUri, null, mSyncManager.getExecutor());
        }, RetryUtils.defaultActiveSyncClientRetry(
            ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT)));
      } else {
        LOG.debug("incremental sync {}", ufsUri.toString());
        RetryUtils.retry("Incremental Sync", () -> {
          mFileSystemMaster.activeSyncMetadata(alluxioUri,
              syncInfo.getChangedFiles(ufsUri).stream()
                  .map((uri) -> Objects.requireNonNull(mMountTable.reverseResolve(uri)).getUri())
                  .collect(Collectors.toSet()),
              mSyncManager.getExecutor());
        }, RetryUtils.defaultActiveSyncClientRetry(
            ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT)));
      }
    } catch (IOException e) {
      LOG.warn("Failed to submit active sync job to master: ufsUri {}, syncPoint {} ", ufsUri,
          alluxioUri, e);
    }
  }
}

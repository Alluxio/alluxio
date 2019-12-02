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

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically sync the files for a particular mountpoint.
 */
@NotThreadSafe
public class ActiveSyncer implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncer.class);
  private final FileSystemMaster mFileSystemMaster;
  private final ActiveSyncManager mSyncManager;
  private final MountTable mMountTable;
  private final long mMountId;

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
  }

  @Override
  public void heartbeat() {
    LOG.debug("start Active Syncer heartbeat");

    List<AlluxioURI> filterList =  mSyncManager.getFilterList(mMountId);

    if (filterList == null || filterList.isEmpty()) {
      return;
    }

    try {
      UfsManager.UfsClient ufsclient = mMountTable.getUfsClient(mMountId);
      try (CloseableResource<UnderFileSystem> ufsResource = ufsclient.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        if (ufs.supportsActiveSync()) {
          SyncInfo syncInfo = ufs.getActiveSyncInfo();
          // This returns a list of ufsUris that we need to sync.
          Set<AlluxioURI> ufsSyncPoints = syncInfo.getSyncPoints();
          // Parallelize across sync points
          List<Callable<Void>> tasksPerSyncPoint = new ArrayList<>(ufsSyncPoints.size());
          for (AlluxioURI ufsUri : ufsSyncPoints) {
            tasksPerSyncPoint.add(() -> {
              processSyncPoint(ufsUri, syncInfo);
              return null;
            });
          }
          mSyncManager.getExecutor().invokeAll(tasksPerSyncPoint);
          // Journal the latest processed txId
          mFileSystemMaster.recordActiveSyncTxid(syncInfo.getTxId(), mMountId);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while submitting active sync change job to master", e);
        Thread.currentThread().interrupt();
        return;
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
            ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT)));
      } else {
        LOG.debug("incremental sync {}", ufsUri.toString());
        RetryUtils.retry("Incremental Sync", () -> {
          mFileSystemMaster.activeSyncMetadata(alluxioUri,
              syncInfo.getChangedFiles(ufsUri).stream().parallel()
                  .map((uri) -> mMountTable.reverseResolve(uri).getUri())
                  .collect(Collectors.toSet()),
              mSyncManager.getExecutor());
        }, RetryUtils.defaultActiveSyncClientRetry(
            ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT)));
      }
    } catch (IOException e) {
      LOG.warn("Failed to submit active sync job to master: ufsUri {}, syncPoint {} ", ufsUri,
          alluxioUri, e);
    }
  }
}

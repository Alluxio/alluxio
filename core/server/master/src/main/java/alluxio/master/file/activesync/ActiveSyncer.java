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
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
  public void heartbeat() throws InterruptedException {
    LOG.debug("start Active Syncer heartbeat");

    List<AlluxioURI> filterList =  mSyncManager.getFilterList(mMountId);
    List<AlluxioURI> ufsUriList = filterList.stream().map(alluxioURI ->
        {
          try {
            return mMountTable.resolve(alluxioURI).getUri();
          } catch (InvalidPathException e) {
            LOG.warn("Invalid path " + alluxioURI.getPath());
            return null;
          }
        }
        ).collect(Collectors.toList());
    if (filterList == null || filterList.isEmpty()) {
      return;
    }

    boolean syncResult = false;
    try {
      UfsManager.UfsClient ufsclient = mMountTable.getUfsClient(mMountId);
      try (CloseableResource<UnderFileSystem> ufsResource = ufsclient.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        if (ufs.supportsActiveSync()) {
          SyncInfo syncInfo = ufs.getActiveSyncInfo();
          // This returns a list of ufsUris that we need to sync.
          Set<AlluxioURI> ufsSyncPoints = syncInfo.getSyncPoints();
          for (AlluxioURI ufsUri : ufsSyncPoints) {

            AlluxioURI alluxioUri = mMountTable.reverseResolve(ufsUri);
            if (alluxioUri != null) {
              if (syncInfo.isForceSync()) {
                LOG.debug("force full sync {}", ufsUri.toString());
                syncResult = mFileSystemMaster.activeSyncMetadata(alluxioUri, null,
                    mSyncManager.getExecutor());
              } else {
                LOG.debug("sync {}", ufsUri.toString());
                syncResult = mFileSystemMaster.activeSyncMetadata(alluxioUri,
                    syncInfo.getChangedFiles(ufsUri).stream().parallel()
                        .map(mMountTable::reverseResolve).collect(Collectors.toSet()),
                    mSyncManager.getExecutor()
                );
              }
              // Journal the latest processed txId
              if (syncResult) {
                mFileSystemMaster.recordActiveSyncTxid(syncInfo.getTxId(), mMountId);
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
}

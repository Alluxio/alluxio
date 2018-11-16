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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Manager for the Active UFS sync process.
 */
public class ActiveSyncManager implements JournalEntryIterable, JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncManager.class);
  private final MountTable mMountTable;

  private final List<AlluxioURI> mSyncPathList;
  // a map which maps mount id to a thread polling that UFS
  private final Map<Long, Future<?>> mPollerMap;
  //  a map which maps each mount id to a list of paths being actively synced on mountpoint
  private final Map<Long, List<AlluxioURI>> mFilterMap;
  
  private FileSystemMaster mFileSystemMaster;

  /**
   * Constructs a Active Sync Manager.
   *
   * @param mountTable mount table
   * @param fileSystemMaster file system master
   */
  public ActiveSyncManager(MountTable mountTable,
      FileSystemMaster fileSystemMaster) {
    mMountTable = mountTable;
    mPollerMap = new ConcurrentHashMap<>();
    mFilterMap = new ConcurrentHashMap<>();
    mSyncPathList = new CopyOnWriteArrayList<>();
    mFileSystemMaster = fileSystemMaster;
  }

  /**
   * Check if a URI is actively synced.
   *
   * @param path path to check
   * @return true if a URI is being actively synced
   */
  public boolean isActivelySynced(AlluxioURI path) throws InvalidPathException {
    for (AlluxioURI syncedPath : mSyncPathList) {
      if (PathUtils.hasPrefix(path.getPath(), syncedPath.getPath())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds a syncpoint from a journal entry.
   *
   * @param syncPoint sync point
   * @return true if sync point successfully added
   */
  public boolean addSyncPointFromJournal(AlluxioURI syncPoint) throws InvalidPathException {
    return startSyncInternal(syncPoint);
  }

  /**
   * start the polling threads.
   *
   * @param executorService executorService to run on
   */
  public void start(ExecutorService executorService) {
    for (long mountId: mFilterMap.keySet()) {
      launchPollingThread(mountId, executorService);
    }
    executorService.submit(
        () -> mSyncPathList.parallelStream().forEach(
            syncPoint -> mFileSystemMaster.batchSyncMetadata(syncPoint, null))
    );
  }

  private void launchPollingThread(long mountId, ExecutorService executorService) {
    if (!mPollerMap.containsKey(mountId)) {
      ActiveSyncer syncer = new ActiveSyncer(mFileSystemMaster, this, mMountTable, mountId);

      Future<?> future = executorService.submit(
          new HeartbeatThread(HeartbeatContext.MASTER_ACTIVE_UFS_SYNC,
              syncer,
              (int) Configuration.getMs(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INTERVAL_MS)));
      mPollerMap.put(mountId, future);
    }
  }

  /**
   * start active sync on a sync point.
   *
   * @param syncPoint sync point
   * @param executorService provided executor service
   * @return true if sync point correctly added
   */
  public boolean startSync(AlluxioURI syncPoint, ExecutorService executorService)
      throws InvalidPathException {
    if (startSyncInternal(syncPoint)) {
      MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
      long mountId = resolution.getMountId();
      launchPollingThread(mountId, executorService);
      // Initial sync
      mFileSystemMaster.batchSyncMetadata(syncPoint, null);
      return true;
    }
    return false;
  }

  private boolean startSyncInternal(AlluxioURI syncPoint) throws InvalidPathException {
    LOG.info("adding syncPoint {}", syncPoint.getPath());
    if (!isActivelySynced(syncPoint)) {
      MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
      long mountId = resolution.getMountId();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        if (!ufsResource.get().supportsActiveSync()) {
          throw new UnsupportedOperationException("Active Syncing is not supported on this UFS type"
              + ufsResource.get().getUnderFSType());
        }
        // Add the new sync point to the filter map
        if (mFilterMap.containsKey(mountId)) {
          mFilterMap.get(mountId).add(syncPoint);
        } else {
          mFilterMap.put(mountId, new CopyOnWriteArrayList<>(Arrays.asList(syncPoint)));
        }
        // Add to the sync point list
        mSyncPathList.add(syncPoint);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * stop active sync on a mount id.
   *
   * @param mountId mountId to stop active sync
   */
  public void stopSync(long mountId) throws InvalidPathException {
    if (mFilterMap.containsKey(mountId)) {
      for (AlluxioURI uri : mFilterMap.get(mountId)) {
        stopSync(uri);
      }
    }
  }

  /**
   * stop active sync on a URI.
   *
   * @param syncPoint stop sync on an active sync
   * @return true if stop sync successfull
   */
  public boolean stopSync(AlluxioURI syncPoint) throws InvalidPathException {
    LOG.info("stop syncPoint {}", syncPoint.getPath());
    for (AlluxioURI uri : mSyncPathList) {
      LOG.info("existing syncpath {}", uri.getPath());
    }
    if (!mSyncPathList.contains(syncPoint)) {
      LOG.info("syncPoint not found {}", syncPoint.getPath());
      return false;
    }
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    long mountId = resolution.getMountId();

    if (mFilterMap.containsKey(mountId)) {
      mFilterMap.get(mountId).remove(syncPoint);
      if (mFilterMap.get(mountId).isEmpty()) {
        // syncPoint removed was the last syncPoint for the rootPath
        mFilterMap.remove(mountId);
        Future<?> future = mPollerMap.remove(mountId);
        if (future != null) {
          future.cancel(true);
        }
      }
      mSyncPathList.remove(syncPoint);
    } else {
      mSyncPathList.remove(syncPoint);
      // We should not be in this situation
      throw new RuntimeException(String.format("mountId for the syncPoint %s not found in the filterMap",
          syncPoint.toString()));
    }
    return true;
  }

  /**
   * Get the filter list associated with mount Id.
   *
   * @param mountId mountId
   * @return a list of URIs (sync points) associated with that mount id
   */
  public List<AlluxioURI> getFilterList(long mountId) {
    return mFilterMap.get(mountId);
  }

  /**
   * Get the sync point list.
   *
   * @return a list of URIs (sync points)
   */
  public List<String> getSyncPathList() {
    return mSyncPathList.stream()
        .map(AlluxioURI::getPath)
        .collect(Collectors.toList());
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    final Iterator<AlluxioURI> it = mSyncPathList.iterator();
    return new Iterator<Journal.JournalEntry>() {
      private AlluxioURI mEntry = null;

      @Override
      public boolean hasNext() {
        if (mEntry != null) {
          return true;
        }
        if (it.hasNext()) {
          mEntry = it.next();
          return true;
        }
        return false;
      }

      @Override
      public Journal.JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        String syncPointPath = mEntry.getPath();
        mEntry = null;

        File.AddSyncPointEntry addSyncPointEntry =
            File.AddSyncPointEntry.newBuilder().setSyncpointPath(syncPointPath)
            .build();

        return Journal.JournalEntry.newBuilder().setAddSyncPoint(addSyncPointEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("ActiveSyncManager#Iterator#remove is not supported.");
      }
    };
  }

  @Override
  public boolean replayJournalEntryFromJournal(Journal.JournalEntry entry) {
    try {
      if (entry.hasAddSyncPoint()) {
        addSyncPointFromJournal(new AlluxioURI(entry.getAddSyncPoint().getSyncpointPath()));
        return true;
      } else if (entry.hasRemoveSyncPoint()) {
        stopSync(new AlluxioURI(entry.getRemoveSyncPoint().getSyncpointPath()));
        return true;
      }
    } catch (AlluxioException e) {
      throw new RuntimeException(e);
    }
    return false;
  }
}

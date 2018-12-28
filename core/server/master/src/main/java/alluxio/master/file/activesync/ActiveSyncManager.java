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
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.SyncInfo;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalEntryReplayable;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddSyncPointEntry;
import alluxio.proto.journal.File.RemoveSyncPointEntry;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.RetryUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.wire.SyncPointInfo;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Manager for the Active UFS sync process.
 */
@NotThreadSafe
public class ActiveSyncManager implements JournalEntryIterable, JournalEntryReplayable {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncManager.class);
  // a reference to the mount table
  private final MountTable mMountTable;
  // a list of sync points
  private final List<AlluxioURI> mSyncPathList;
  // a map which maps mount id to a thread polling that UFS
  private final Map<Long, Future<?>> mPollerMap;
  //  a map which maps each mount id to a list of paths being actively synced on mountpoint
  private final Map<Long, List<AlluxioURI>> mFilterMap;
  // a map which maps mount id to the latest txid synced on that mount point
  private final Map<Long, Long> mStartingTxIdMap;
  // Future.isDone = INITIALLY_SYNCED, !Future.isDone = SYNCING
  // Future == null => NOT_INITIALLY_SYNCED
  private final Map<AlluxioURI, Future<?>> mSyncPathStatus;
  // a lock which protects the above data structures
  private final Lock mSyncManagerLock;
  // a reference to FSM
  private FileSystemMaster mFileSystemMaster;
  // a local executor service used to launch polling threads
  private ExecutorService mExecutorService;

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
    mStartingTxIdMap = new ConcurrentHashMap<>();
    mSyncPathList = new CopyOnWriteArrayList<>();
    mFileSystemMaster = fileSystemMaster;
    mSyncPathStatus = new ConcurrentHashMap<>();

    // A lock used to protect the state stored in the above maps and lists
    mSyncManagerLock = new ReentrantLock();
    // Executor Service for active syncing
    mExecutorService =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  /**
   * Check if a URI is actively synced.
   *
   * @param path path to check
   * @return true if a URI is being actively synced
   */
  public boolean isActivelySynced(AlluxioURI path) {
    for (AlluxioURI syncedPath : mSyncPathList) {
      try {
        if (PathUtils.hasPrefix(path.getPath(), syncedPath.getPath())) {
          return true;
        }
      } catch (InvalidPathException e) {
        return false;
      }
    }
    return false;
  }

  /**
   * Adds a syncpoint from a journal entry.
   *
   * @param syncPoint sync point
   * @param mountId mount id
   * @return true if sync point successfully added
   */
  public boolean addSyncPointFromJournal(AlluxioURI syncPoint, long mountId)
      throws InvalidPathException, IOException {
    return startSyncFromJournal(syncPoint, mountId);
  }

  /**
   * Gets the lock protecting the syncManager.
   *
   * @return syncmanager lock
   */
  public Lock getSyncManagerLock() {
    return mSyncManagerLock;
  }

  /**
   * start the polling threads.
   *
   */
  public void start() throws IOException {
    // Initialize UFS states
    for (AlluxioURI syncPoint : mSyncPathList) {

      MountTable.Resolution resolution = null;
      long mountId = 0;
      try {
        resolution = mMountTable.resolve(syncPoint);
        mountId = resolution.getMountId();
      } catch (InvalidPathException e) {
        LOG.info("Invalid Path encountered during start up of ActiveSyncManager, "
            + "path {}, exception {}", syncPoint, e);
        continue;
      }

      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        if (!ufsResource.get().supportsActiveSync()) {
          throw new UnsupportedOperationException("Active Sync is not supported on this UFS type "
              + ufsResource.get().getUnderFSType());
        }
        ufsResource.get().startSync(resolution.getUri());
      }
    }
    // attempt to restart from a past txid, if this fails, it will result in MissingEventException
    // therefore forces a sync
    for (long mountId: mFilterMap.keySet()) {
      long txId = mStartingTxIdMap.containsKey(mountId)
          ? mStartingTxIdMap.get(mountId) : SyncInfo.INVALID_TXID;
      launchPollingThread(mountId, txId);

      try {
        if ((txId == SyncInfo.INVALID_TXID)
            && Configuration.getBoolean(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INITIAL_SYNC)) {
          mExecutorService.submit(
              () -> mFilterMap.get(mountId).parallelStream().forEach(
                  syncPoint -> {
                    try {
                      RetryUtils.retry("active sync during start",
                          () -> mFileSystemMaster.activeSyncMetadata(syncPoint,
                              null, getExecutor()),
                          RetryUtils.defaultActiveSyncClientRetry());
                    } catch (IOException e) {
                      LOG.warn("IOException encountered during active sync while starting {}", e);
                    }
                  }
          )).get();
        }
      } catch (Exception e) {
        LOG.warn("exception encountered during initial sync {}", e);
      }
    }
  }

  /**
   * Launches polling thread on a particular mount point with starting txId.
   *
   * @param mountId launch polling thread on a mount id
   * @param txId specifies the transaction id to initialize the pollling thread
   */
  public void launchPollingThread(long mountId, long txId) {
    if (!mPollerMap.containsKey(mountId)) {
      try (CloseableResource<UnderFileSystem> ufsClient =
               mMountTable.getUfsClient(mountId).acquireUfsResource()) {
        ufsClient.get().startActiveSyncPolling(txId);
      } catch (IOException e) {
        LOG.warn("IO Exception trying to launch Polling thread {}", e);
      }
      ActiveSyncer syncer = new ActiveSyncer(mFileSystemMaster, this, mMountTable, mountId);
      Future<?> future = getExecutor().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_ACTIVE_UFS_SYNC,
              syncer,
              (int) Configuration.getMs(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INTERVAL)));
      mPollerMap.put(mountId, future);
    }
  }

  /**
   * Apply AddSyncPoint entry and journal the entry.
   *
   * @param context journal context
   * @param entry addSyncPoint entry
   * @param resolution mount table resolution for the sync point contained in the entry
   * @return true if successfully applied and journaled
   */
  public boolean applyAndJournal(Supplier<JournalContext> context, AddSyncPointEntry entry,
      MountTable.Resolution resolution) {
    AlluxioURI uri = new AlluxioURI(entry.getSyncpointPath());
    boolean launchPollingThread;
    try {
      launchPollingThread = startSyncInternal(uri, resolution);
      context.get().append(Journal.JournalEntry.newBuilder().setAddSyncPoint(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }

    return launchPollingThread;
  }

  /**
   * Apply removeSyncPoint entry and journal the entry.
   *
   * @param context journal context
   * @param entry removeSyncPoint entry
   * @param resolution mount table resolution for the sync point contained in the entry
   */
  public void applyAndJournal(Supplier<JournalContext> context,
      RemoveSyncPointEntry entry, MountTable.Resolution resolution) {
    AlluxioURI uri = new AlluxioURI(entry.getSyncpointPath());
    try {
      stopSyncInternal(uri, resolution);
      context.get().append(Journal.JournalEntry.newBuilder().setRemoveSyncPoint(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  private boolean startSyncFromJournal(AlluxioURI syncPoint, long mountId)
      throws InvalidPathException, IOException {
    LOG.debug("adding syncPoint {}", syncPoint.getPath());
    if (!isActivelySynced(syncPoint)) {
      // Add the new sync point to the filter map
      if (mFilterMap.containsKey(mountId)) {
        mFilterMap.get(mountId).add(syncPoint);
      } else {
        mFilterMap.put(mountId, new CopyOnWriteArrayList<>(Arrays.asList(syncPoint)));
      }
      // Add to the sync point list
      mSyncPathList.add(syncPoint);
      return true;
    } else {
      return false;
    }
  }

  private boolean startSyncInternal(AlluxioURI syncPoint, MountTable.Resolution resolution) {
    LOG.debug("adding syncPoint {}", syncPoint.getPath());
    if (!isActivelySynced(syncPoint)) {
      long mountId = resolution.getMountId();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        Future<?> syncFuture = mExecutorService.submit(
            () -> {
              try {
                ufsResource.get().startSync(resolution.getUri());
              } catch (IOException e) {
                LOG.info(ExceptionMessage.FAILED_INITIAL_SYNC.getMessage(
                    resolution.getUri()), e);
              }
            });
        mSyncPathStatus.put(syncPoint, syncFuture);
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
  public void stopSyncForMount(long mountId) throws InvalidPathException, IOException {
    if (mFilterMap.containsKey(mountId)) {
      for (AlluxioURI uri : mFilterMap.get(mountId)) {
        MountTable.Resolution resolution = stopSyncCheck(uri);
        stopSyncInternal(uri, resolution);
      }
    }
  }

  /**
   * Perform various checks of stopping a sync point.
   *
   * @param syncPoint sync point to stop
   * @return the path resolution result if successfully passed all checks
   */
  public MountTable.Resolution stopSyncCheck(AlluxioURI syncPoint) throws InvalidPathException {
    if (!mSyncPathList.contains(syncPoint)) {
      LOG.debug("syncPoint not found {}", syncPoint.getPath());
      throw new InvalidPathException("Sync Point Not Found" + syncPoint);
    }
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    return resolution;
  }
  /**
   * stop active sync on a URI.
   *
   * @param syncPoint sync point to be stopped
   * @param resolution path resolution for the sync point
   * @return true if stop sync successfull
   */
  public boolean stopSyncInternal(AlluxioURI syncPoint, MountTable.Resolution resolution) {
    try (LockResource r = new LockResource(mSyncManagerLock)) {
      LOG.debug("stop syncPoint {}", syncPoint.getPath());
      long mountId = resolution.getMountId();

      if (mFilterMap.containsKey(mountId)) {
        mFilterMap.get(mountId).remove(syncPoint);
        mSyncPathList.remove(syncPoint);
        Future<?> syncFuture = mSyncPathStatus.remove(syncPoint);
        if (syncFuture != null) {
          syncFuture.cancel(true);
        }
        try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
          ufs.get().stopSync(resolution.getUri());
        } catch (IOException e) {
          LOG.info("Ufs IOException for uri {}, exception is {}", syncPoint, e);
        }

        if (mFilterMap.get(mountId).isEmpty()) {
          // syncPoint removed was the last syncPoint for the mountId
          mFilterMap.remove(mountId);

          Future<?> future = mPollerMap.remove(mountId);
          if (future != null) {
            future.cancel(true);
          }

          try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
            ufs.get().stopActiveSyncPolling();
          } catch (IOException e) {
            LOG.warn("Encountered IOException when trying to stop polling thread {}", e);
          }
        }
      } else {
        mSyncPathList.remove(syncPoint);
        try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
          ufs.get().stopSync(resolution.getUri());
        } catch (IOException e) {
          LOG.info("Ufs IOException for uri {}, exception is {}", syncPoint, e);
        }

        // We should not be in this situation
        throw new RuntimeException(
            String.format("mountId for the syncPoint %s not found in the filterMap",
            syncPoint.toString()));
      }
      return true;
    }
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
  public List<SyncPointInfo> getSyncPathList() {
    List<SyncPointInfo> returnList = new ArrayList<>();
    for (AlluxioURI uri: mSyncPathList) {
      SyncPointInfo.SyncStatus status;
      Future<?> syncStatus = mSyncPathStatus.get(uri);
      if (syncStatus == null) {
        status = SyncPointInfo.SyncStatus.NOT_INITIALLY_SYNCED;
      } else if (syncStatus.isDone()) {
        status = SyncPointInfo.SyncStatus.INITIALLY_SYNCED;
      } else {
        status = SyncPointInfo.SyncStatus.SYNCING;
      }
      returnList.add(new SyncPointInfo(uri, status));
    }
    return returnList;
  }

  private Iterator<Journal.JournalEntry> getSyncPathIterator() {
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
        long mountId = 0;
        try {
          MountTable.Resolution resolution = mMountTable.resolve(mEntry);
          mountId = resolution.getMountId();
        } catch (InvalidPathException e) {
          LOG.info("Path resolution failed for {}, exception {}", syncPointPath, e);
        }
        mEntry = null;

        File.AddSyncPointEntry addSyncPointEntry =
            File.AddSyncPointEntry.newBuilder()
                .setSyncpointPath(syncPointPath)
                .setMountId(mountId)
                .build();

        return Journal.JournalEntry.newBuilder().setAddSyncPoint(addSyncPointEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "ActiveSyncManager#Iterator#remove is not supported.");
      }
    };
  }

  @Override
  public boolean replayJournalEntryFromJournal(Journal.JournalEntry entry) {
    try {
      if (entry.hasAddSyncPoint()) {
        addSyncPointFromJournal(new AlluxioURI(entry.getAddSyncPoint().getSyncpointPath()),
            entry.getAddSyncPoint().getMountId());
        return true;
      } else if (entry.hasRemoveSyncPoint()) {
        AlluxioURI uri = new AlluxioURI(entry.getRemoveSyncPoint().getSyncpointPath());
        MountTable.Resolution resolution = stopSyncCheck(uri);
        stopSyncInternal(uri, resolution);
        return true;
      } else if (entry.hasActiveSyncTxId()) {
        File.ActiveSyncTxIdEntry activeSyncTxId = entry.getActiveSyncTxId();
        setTxId(activeSyncTxId.getMountId(), activeSyncTxId.getTxId());
        return true;
      }
    } catch (AlluxioException | IOException e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  private Iterator<Journal.JournalEntry> getTxIdIterator() {
    final Iterator<Map.Entry<Long, Long>> it = mStartingTxIdMap.entrySet().iterator();
    return new Iterator<Journal.JournalEntry>() {
      private Map.Entry<Long, Long> mEntry = null;

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
        long mountId = mEntry.getKey();
        long txId = mEntry.getValue();
        mEntry = null;

        File.ActiveSyncTxIdEntry txIdEntry =
            File.ActiveSyncTxIdEntry.newBuilder().setMountId(mountId)
                .setTxId(txId).build();
        return Journal.JournalEntry.newBuilder().setActiveSyncTxId(txIdEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "ActiveSyncManager#Iterator#remove is not supported.");
      }
    };
  }

  @Override
  public Iterator<Journal.JournalEntry> getJournalEntryIterator() {
    return Iterators.concat(getSyncPathIterator(), getTxIdIterator());
  }

  /**
   * set the transaction id for a particular mountId.
   *
   * @param mountId mount id
   * @param txId transaction id
   */
  public void setTxId(long mountId, long txId) {
    mStartingTxIdMap.put(mountId, txId);
  }

  /**
   * Get SyncManager Executor.
   *
   * @return an executor for active syncing
   */
  public ExecutorService getExecutor() {
    return mExecutorService;
  }

  /**
   * Stop the sync manager.
   */
  public void stop() {
    for (long mountId : mFilterMap.keySet()) {
      try {
        stopSyncForMount(mountId);
      } catch (IOException e) {
        LOG.info("ActiveSyncManager stop: IOException stopping mountId {}, exception {}",
            mountId,  e);
      } catch (InvalidPathException e) {
        LOG.info("ActiveSyncManager stop: InvalidPathException stopping mountId {}, exception {}",
            mountId,  e);
      }
    }
  }
}

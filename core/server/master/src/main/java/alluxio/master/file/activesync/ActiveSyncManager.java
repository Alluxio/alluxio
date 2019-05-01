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
import alluxio.ProcessUtils;
import alluxio.SyncInfo;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddSyncPointEntry;
import alluxio.proto.journal.File.RemoveSyncPointEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.RetryUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.wire.SyncPointInfo;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manager for the Active UFS sync process.
 * There are several threads cooperating to make the active sync process happen.
 * 1. An active polling thread that polls HDFS for change events and aggregates these events.
 * 2. A heartbeat thread that wakes up periodically to consume the aggregated events, and perform
 *    syncing if necessary.
 * 3. For initial syncing, we launch a future to perform initial syncing asynchronously. This is
 *    stored in mSyncPathStatus.
 */
@NotThreadSafe
public class ActiveSyncManager implements Journaled {
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
  public ActiveSyncManager(MountTable mountTable, FileSystemMaster fileSystemMaster) {
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
          throw new UnsupportedOperationException("Active Sync is not supported on this UFS type: "
              + ufsResource.get().getUnderFSType());
        }
        ufsResource.get().startSync(resolution.getUri());
      }
    }
    // attempt to restart from a past txid, if this fails, it will result in MissingEventException
    // therefore forces a sync
    for (long mountId: mFilterMap.keySet()) {
      long txId = mStartingTxIdMap.getOrDefault(mountId, SyncInfo.INVALID_TXID);
      launchPollingThread(mountId, txId);

      try {
        if ((txId == SyncInfo.INVALID_TXID)
            && ServerConfiguration.getBoolean(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INITIAL_SYNC)) {
          mExecutorService.submit(
              () -> mFilterMap.get(mountId).parallelStream().forEach(
                  syncPoint -> {
                    try {
                      RetryUtils.retry("active sync during start",
                          () -> mFileSystemMaster.activeSyncMetadata(syncPoint,
                              null, getExecutor()),
                          RetryUtils.defaultActiveSyncClientRetry(ServerConfiguration
                              .getMs(PropertyKey.MASTER_ACTIVE_UFS_POLL_TIMEOUT)));
                    } catch (IOException e) {
                      LOG.warn("IOException encountered during active sync while starting {}", e);
                    }
                  }
          ));
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
    LOG.debug("launch polling thread for mount id {}, txId {}", mountId, txId);
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
              syncer, (int) ServerConfiguration.getMs(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INTERVAL),
              ServerConfiguration.global()));
      mPollerMap.put(mountId, future);
    }
  }

  /**
   * Apply AddSyncPoint entry and journal the entry.
   *  @param context journal context
   * @param entry addSyncPoint entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, AddSyncPointEntry entry) {
    try {
      apply(entry);
      context.get().append(Journal.JournalEntry.newBuilder().setAddSyncPoint(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Apply removeSyncPoint entry and journal the entry.
   * @param context journal context
   * @param entry removeSyncPoint entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, RemoveSyncPointEntry entry) {
    try {
      apply(entry);
      context.get().append(Journal.JournalEntry.newBuilder().setRemoveSyncPoint(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * stop active sync on a mount id.
   *
   * @param mountId mountId to stop active sync
   */
  public void stopSyncForMount(long mountId) throws InvalidPathException, IOException {
    LOG.debug("Stop sync for mount id {}", mountId);
    if (mFilterMap.containsKey(mountId)) {
      List<Pair<AlluxioURI, MountTable.Resolution>> toBeDeleted = new ArrayList<>();
      for (AlluxioURI uri : mFilterMap.get(mountId)) {
        MountTable.Resolution resolution = resolveSyncPoint(uri);
        if (resolution != null) {
          toBeDeleted.add(new Pair<>(uri, resolution));
        }
      }
      // Calling stopSyncInternal outside of the traversal of mFilterMap.get(mountId) to avoid
      // ConcurrentModificationException
      for (Pair<AlluxioURI, MountTable.Resolution> deleteInfo : toBeDeleted) {
        stopSyncInternal(deleteInfo.getFirst(), deleteInfo.getSecond());
      }
    }
  }

  /**
   * Perform various checks of stopping a sync point.
   *
   * @param syncPoint sync point to stop
   * @return the path resolution result if successfully passed all checks
   */

  @Nullable
  public MountTable.Resolution resolveSyncPoint(AlluxioURI syncPoint) throws InvalidPathException {
    if (!mSyncPathList.contains(syncPoint)) {
      LOG.debug("syncPoint not found {}", syncPoint.getPath());
      return null;
    }
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    return resolution;
  }
  /**
   * stop active sync on a URI.
   *
   * @param syncPoint sync point to be stopped
   * @param resolution path resolution for the sync point
   */
  public void stopSyncInternal(AlluxioURI syncPoint, MountTable.Resolution resolution) {
    try (LockResource r = new LockResource(mSyncManagerLock)) {
      LOG.debug("stop syncPoint {}", syncPoint.getPath());
      RemoveSyncPointEntry removeSyncPoint = File.RemoveSyncPointEntry.newBuilder()
          .setSyncpointPath(syncPoint.toString())
          .setMountId(resolution.getMountId())
          .build();
      apply(removeSyncPoint);
      try {
        stopSyncPostJournal(syncPoint);
      } catch (Throwable e) {
        // revert state;
        AddSyncPointEntry addSyncPoint =
            File.AddSyncPointEntry.newBuilder()
                .setSyncpointPath(syncPoint.toString()).build();
        apply(addSyncPoint);
        recoverFromStopSync(syncPoint, resolution.getMountId());
      }
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
        long mountId = -1;
        while (mountId == -1) {
          try {
            syncPointPath = mEntry.getPath();
            MountTable.Resolution resolution = mMountTable.resolve(mEntry);
            mountId = resolution.getMountId();
          } catch (InvalidPathException e) {
            LOG.info("Path resolution failed for {}, exception {}", syncPointPath, e);
            mEntry = null;
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
          }
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

  private void apply(RemoveSyncPointEntry removeSyncPoint) {
    AlluxioURI syncPoint = new AlluxioURI(removeSyncPoint.getSyncpointPath());
    long mountId = removeSyncPoint.getMountId();

    try (LockResource r = new LockResource(mSyncManagerLock)) {
      LOG.debug("stop syncPoint {}", syncPoint.getPath());

      if (mFilterMap.containsKey(mountId)) {
        List list = mFilterMap.get(mountId);
        if (list != null) {
          list.remove(syncPoint);
        }
        mSyncPathList.remove(syncPoint);
      } else {
        mSyncPathList.remove(syncPoint);
        // We should not be in this situation
        throw new RuntimeException(
            String.format("mountId for the syncPoint %s not found in the filterMap",
                syncPoint.toString()));
      }
    }
  }

  private void apply(AddSyncPointEntry addSyncPoint) {
    AlluxioURI syncPoint = new AlluxioURI(addSyncPoint.getSyncpointPath());
    long mountId = addSyncPoint.getMountId();

    LOG.debug("adding syncPoint {}, mount id {}", syncPoint.getPath(), mountId);
    // Add the new sync point to the filter map
    if (mFilterMap.containsKey(mountId)) {
      mFilterMap.get(mountId).add(syncPoint);
    } else {
      ArrayList<AlluxioURI> list = new ArrayList<>();
      list.add(syncPoint);
      mFilterMap.put(mountId, list);
    }
    // Add to the sync point list
    mSyncPathList.add(syncPoint);
  }

  /**
   * Clean up tasks to stop sync point after we have journaled.
   *
   * @param syncPoint the sync point to stop
   * @throws InvalidPathException
   */
  public void stopSyncPostJournal(AlluxioURI syncPoint) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    long mountId = resolution.getMountId();
    // Remove initial sync thread
    Future<?> syncFuture = mSyncPathStatus.remove(syncPoint);
    if (syncFuture != null) {
      syncFuture.cancel(true);
    }

    if (mFilterMap.get(mountId).isEmpty()) {
      // syncPoint removed was the last syncPoint for the mountId
      mFilterMap.remove(mountId);
      Future<?> future = mPollerMap.remove(mountId);
      if (future != null) {
        future.cancel(true);
      }
    }

    // Tell UFS to stop monitoring the path
    try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
      ufs.get().stopSync(resolution.getUri());
    } catch (IOException e) {
      LOG.info("Ufs IOException for uri {}, exception is {}", syncPoint, e);
    }

    // Stop active sync polling on a particular UFS if it is the last sync point
    if (mFilterMap.containsKey(mountId) && mFilterMap.get(mountId).isEmpty()) {
      try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
        ufs.get().stopActiveSyncPolling();
      } catch (IOException e) {
        LOG.warn("Encountered IOException when trying to stop polling thread {}", e);
      }
    }
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
   * Stops the sync manager and any outstanding threads, does not change the sync points.
   * This stops four things in the following order.
   * 1. Stop any outstanding initial sync futures for the sync points. (syncFuture.cancel)
   * 2. Stop the heartbeat thread that periodically wakes up to process events that have been
   *    recorded for the past heartbeat interval.
   * 3. Tell the polling thread to stop monitoring the path for events
   * 4. Stop the thread that is polling HDFS for events
   */
  public void stop() {
    for (AlluxioURI syncPoint : mSyncPathList) {
      MountTable.Resolution resolution = null;
      try {
        resolution = mMountTable.resolve(syncPoint);
      } catch (InvalidPathException e) {
        LOG.warn("stop: InvalidPathException resolving syncPoint {}, exception {}",
            syncPoint,  e);
      }
      long mountId = resolution.getMountId();
      // Remove initial sync thread
      Future<?> syncFuture = mSyncPathStatus.remove(syncPoint);
      if (syncFuture != null) {
        syncFuture.cancel(true);
      }

      Future<?> future = mPollerMap.remove(mountId);
      if (future != null) {
        future.cancel(true);
      }

      // Tell UFS to stop monitoring the path
      try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
        ufs.get().stopSync(resolution.getUri());
      } catch (IOException e) {
        LOG.warn("Ufs IOException for uri {}, exception is {}", syncPoint, e);
      }

      try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
        ufs.get().stopActiveSyncPolling();
      } catch (IOException e) {
        LOG.warn("Encountered IOException when trying to stop polling thread {}", e);
      }
    }
  }

  private void startInitSync(AlluxioURI uri, MountTable.Resolution resolution) {
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      Future<?> syncFuture = mExecutorService.submit(
          () -> {
            try {
              // Notify ufs polling thread to keep track of events related to specified uri
              ufsResource.get().startSync(resolution.getUri());
              // Start the initial metadata sync between the ufs and alluxio for the specified uri
              if (ServerConfiguration.getBoolean(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INITIAL_SYNC)) {
                mFileSystemMaster.activeSyncMetadata(uri, null, getExecutor());
              }
            } catch (IOException e) {
              LOG.info(ExceptionMessage.FAILED_INITIAL_SYNC.getMessage(
                  resolution.getUri()), e);
            }
          });

      mSyncPathStatus.put(uri, syncFuture);
    }
  }

  /**
   * Continue to start sync after we have journaled the operation.
   *
   * @param uri the sync point that we are trying to start
   */
  public void startSyncPostJournal(AlluxioURI uri) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(uri);
    startInitSync(uri, resolution);
    launchPollingThread(resolution.getMountId(), SyncInfo.INVALID_TXID);
  }

  /**
   * Recover from a stop sync operation.
   *
   * @param uri uri to stop sync
   * @param mountId mount id of the uri
   */
  public void recoverFromStopSync(AlluxioURI uri, long mountId) {
    if (mSyncPathStatus.containsKey(uri)) {
      // nothing to recover from, since the syncPathStatus still contains syncPoint
      return;
    }
    try {
      // the init sync thread has been removed, to reestablish sync, we need to sync again
      MountTable.Resolution resolution = mMountTable.resolve(uri);
      startInitSync(uri, resolution);
      launchPollingThread(resolution.getMountId(), SyncInfo.INVALID_TXID);
    } catch (Throwable t) {
      LOG.warn("Recovering from stop syncing failed {}", t);
    }
  }

  /**
   * Recover from start sync operation.
   *
   * @param uri uri to start sync
   * @param mountId mount id of the uri
   */
  public void recoverFromStartSync(AlluxioURI uri, long mountId) {
    // if the init sync has been launched, we need to stop it
    if (mSyncPathStatus.containsKey(uri)) {
      Future<?> syncFuture = mSyncPathStatus.remove(uri);
      if (syncFuture != null) {
        syncFuture.cancel(true);
      }
    }

    // if the polling thread has been launched, we need to stop it
    mFilterMap.remove(mountId);
    Future<?> future = mPollerMap.remove(mountId);
    if (future != null) {
      future.cancel(true);
    }
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    if (entry.hasAddSyncPoint()) {
      apply(entry.getAddSyncPoint());
      return true;
    } else if (entry.hasRemoveSyncPoint()) {
      apply(entry.getRemoveSyncPoint());
      return true;
    } else if (entry.hasActiveSyncTxId()) {
      File.ActiveSyncTxIdEntry activeSyncTxId = entry.getActiveSyncTxId();
      setTxId(activeSyncTxId.getMountId(), activeSyncTxId.getTxId());
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * It clears all sync points, and stops the polling thread.
   */
  @Override
  public void resetState() {
    for (long mountId : mFilterMap.keySet()) {
      try {
        // stops sync point under this mount point. Note this clears the sync point and
        // stops associated polling threads.
        stopSyncForMount(mountId);
      } catch (IOException | InvalidPathException e) {
        LOG.info("Exception resetting mountId {}, exception: {}", mountId, e);
      }
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.ACTIVE_SYNC_MANAGER;
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.concat(getSyncPathIterator(), getTxIdIterator());
  }
}

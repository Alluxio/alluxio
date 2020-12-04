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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.RpcContext;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddSyncPointEntry;
import alluxio.proto.journal.File.RemoveSyncPointEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.SyncPointInfo;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manager for the Active UFS sync process.
 *
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
  private final Lock mLock;
  // a reference to FSM
  private final FileSystemMaster mFileSystemMaster;
  // a local executor service used to launch polling threads
  private final ThreadPoolExecutor mExecutorService;
  private boolean mStarted;

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
    mLock = new ReentrantLock();
    // Executor Service for active syncing
    mExecutorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
        Runtime.getRuntime().availableProcessors(),
        1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
        ThreadFactoryUtils.build("ActiveSyncManager-%d", false));
    mExecutorService.allowCoreThreadTimeOut(true);
  }

  /**
   * Gets the lock protecting the syncManager.
   *
   * @return syncmanager lock
   */
  public Lock getLock() {
    return mLock;
  }

  /**
   * @param syncPoint the uri to check
   * @return true if the given path is a sync point
   */
  public boolean isSyncPoint(AlluxioURI syncPoint) {
    return mSyncPathList.contains(syncPoint);
  }

  /**
   * Check if a URI is actively synced.
   *
   * @param path path to check
   * @return true if a URI is being actively synced
   */
  public boolean isUnderSyncPoint(AlluxioURI path) {
    for (AlluxioURI syncedPath : mSyncPathList) {
      try {
        if (PathUtils.hasPrefix(path.getPath(), syncedPath.getPath())
            && mMountTable.getMountPoint(path).equals(mMountTable.getMountPoint(syncedPath))) {
          return true;
        }
      } catch (InvalidPathException e) {
        return false;
      }
    }
    return false;
  }

  /**
   * Start the polling threads.
   */
  public void start() throws IOException {
    mStarted = true;
    // Initialize UFS states
    for (AlluxioURI syncPoint : mSyncPathList) {
      MountTable.Resolution resolution;
      try {
        resolution = mMountTable.resolve(syncPoint);
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
    for (Map.Entry<Long, List<AlluxioURI>> entry : mFilterMap.entrySet()) {
      long mountId = entry.getKey();
      long txId = mStartingTxIdMap.getOrDefault(mountId, SyncInfo.INVALID_TXID);
      if (!entry.getValue().isEmpty()) {
        launchPollingThread(mountId, txId);
      }

      try {
        if ((txId == SyncInfo.INVALID_TXID) && ServerConfiguration.getBoolean(
            PropertyKey.MASTER_UFS_ACTIVE_SYNC_INITIAL_SYNC_ENABLED)) {
          mExecutorService.submit(
              () -> entry.getValue().parallelStream().forEach(
                  syncPoint -> {
                    MountTable.Resolution resolution;
                    try {
                      resolution = mMountTable.resolve(syncPoint);
                    } catch (InvalidPathException e) {
                      LOG.info("Invalid Path encountered during start up of ActiveSyncManager, "
                          + "path {}, exception {}", syncPoint, e);
                      return;
                    }
                    startInitialFullSync(syncPoint, resolution);
                  }
              ));
        }
      } catch (Exception e) {
        LOG.warn("exception encountered during initial sync: {}", e.toString());
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
      UfsManager.UfsClient ufsClient = mMountTable.getUfsClient(mountId);
      if (ufsClient == null) {
        LOG.warn("Mount id {} does not exist", mountId);
        return;
      }
      try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
        ufsResource.get().startActiveSyncPolling(txId);
      } catch (IOException e) {
        LOG.warn("IO Exception trying to launch Polling thread: {}", e.toString());
      }
      ActiveSyncer syncer = new ActiveSyncer(mFileSystemMaster, this, mMountTable, mountId);
      Future<?> future = getExecutor().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_ACTIVE_UFS_SYNC,
              syncer, (int) ServerConfiguration.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_INTERVAL),
              ServerConfiguration.global(), ServerUserState.global()));
      mPollerMap.put(mountId, future);
    }
  }

  /**
   * Apply {@link AddSyncPointEntry} and journal the entry.
   *
   * @param context journal context
   * @param entry addSyncPoint entry
   */
  public void applyAndJournal(Supplier<JournalContext> context, AddSyncPointEntry entry) {
    LOG.info("Apply startSync {}", entry.getSyncpointPath());
    try {
      apply(entry);
      context.get().append(Journal.JournalEntry.newBuilder().setAddSyncPoint(entry).build());
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to apply %s", entry);
      throw t; // fatalError will usually system.exit
    }
  }

  /**
   * Apply {@link RemoveSyncPointEntry} and journal the entry.
   *
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
   * Start active sync on a URI and journal the add entry.
   *
   * @param rpcContext the master rpc or no-op context
   * @param syncPoint sync point to be start
   */
  public void startSyncAndJournal(RpcContext rpcContext, AlluxioURI syncPoint)
      throws InvalidPathException {
    try (LockResource r = new LockResource(mLock)) {
      MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
      long mountId = resolution.getMountId();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        if (!ufsResource.get().supportsActiveSync()) {
          throw new UnsupportedOperationException(
              "Active Syncing is not supported on this UFS type: "
                  + ufsResource.get().getUnderFSType());
        }
      }

      if (isUnderSyncPoint(syncPoint)) {
        throw new InvalidPathException("URI " + syncPoint + " is already a sync point");
      }
      AddSyncPointEntry addSyncPoint =
          AddSyncPointEntry.newBuilder()
              .setSyncpointPath(syncPoint.toString())
              .setMountId(mountId)
              .build();
      applyAndJournal(rpcContext, addSyncPoint);
      try {
        startSyncInternal(syncPoint, resolution);
      } catch (Throwable e) {
        LOG.warn("Start sync failed on {}", syncPoint, e);
        // revert state;
        RemoveSyncPointEntry removeSyncPoint =
            File.RemoveSyncPointEntry.newBuilder()
                .setSyncpointPath(syncPoint.toString()).build();
        applyAndJournal(rpcContext, removeSyncPoint);
        recoverFromStartSync(syncPoint, resolution.getMountId());
        throw e;
      }
    }
  }

  /**
   * Stop active sync on a mount id.
   *
   * @param mountId mountId to stop active sync
   */
  public void stopSyncForMount(long mountId) throws InvalidPathException {
    LOG.info("Stop sync for mount id {}", mountId);
    if (mFilterMap.containsKey(mountId)) {
      List<AlluxioURI> toBeDeleted = new ArrayList<>(mFilterMap.get(mountId));
      for (AlluxioURI uri : toBeDeleted) {
        stopSyncAndJournal(RpcContext.NOOP, uri);
      }
    }
  }

  /**
   * Stop active sync on a URI and journal the remove entry.
   *
   * @param rpcContext the master rpc or no-op context
   * @param syncPoint sync point to be stopped
   */
  public void stopSyncAndJournal(RpcContext rpcContext, AlluxioURI syncPoint)
      throws InvalidPathException {
    if (!isSyncPoint(syncPoint)) {
      throw new InvalidPathException(String.format("%s is not a sync point", syncPoint));
    }
    try (LockResource r = new LockResource(mLock)) {
      MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
      LOG.debug("stop syncPoint {}", syncPoint.getPath());
      final long mountId = resolution.getMountId();
      RemoveSyncPointEntry removeSyncPoint = File.RemoveSyncPointEntry.newBuilder()
          .setSyncpointPath(syncPoint.toString())
          .setMountId(mountId)
          .build();
      applyAndJournal(rpcContext, removeSyncPoint);
      try {
        stopSyncInternal(syncPoint);
      } catch (Throwable e) {
        LOG.warn("Stop sync failed on {}", syncPoint, e);
        // revert state;
        AddSyncPointEntry addSyncPoint =
            File.AddSyncPointEntry.newBuilder()
                .setSyncpointPath(syncPoint.toString()).build();
        applyAndJournal(rpcContext, addSyncPoint);
        recoverFromStopSync(syncPoint);
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
            String mountPoint = mMountTable.getMountPoint(mEntry);
            MountInfo mountInfo = mMountTable.getMountTable().get(mountPoint);
            mountId = mountInfo.getMountId();
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

    try (LockResource r = new LockResource(mLock)) {
      LOG.info("SyncPoint stopped {}", syncPoint.getPath());

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

    LOG.info("SyncPoint added {}, mount id {}", syncPoint.getPath(), mountId);
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
   * Continue to start sync after we have journaled the operation.
   *
   * @param syncPoint the sync point that we are trying to start
   * @param resolution the mount table resolution of sync point
   */
  private void startSyncInternal(AlluxioURI syncPoint, MountTable.Resolution resolution) {
    startInitialFullSync(syncPoint, resolution);
    launchPollingThread(resolution.getMountId(), SyncInfo.INVALID_TXID);
  }

  /**
   * Start a thread for an initial full sync.
   *
   * @param syncPoint the sync point
   * @param resolution the mount table resolution
   */
  private void startInitialFullSync(AlluxioURI syncPoint, MountTable.Resolution resolution) {
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      Future<?> syncFuture = mExecutorService.submit(
          () -> {
            try {
              // Notify ufs polling thread to keep track of events related to specified uri
              ufsResource.get().startSync(resolution.getUri());
              // Start the initial metadata sync between the ufs and alluxio for the specified uri
              if (ServerConfiguration.getBoolean(
                  PropertyKey.MASTER_UFS_ACTIVE_SYNC_INITIAL_SYNC_ENABLED)) {
                RetryUtils.retry("active sync during start",
                    () -> mFileSystemMaster.activeSyncMetadata(syncPoint,
                        null, getExecutor()),
                    RetryUtils.defaultActiveSyncClientRetry(ServerConfiguration
                        .getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_RETRY_TIMEOUT)));
              }
            } catch (IOException e) {
              LOG.info(ExceptionMessage.FAILED_INITIAL_SYNC.getMessage(
                  resolution.getUri()), e);
            }
          });
      mSyncPathStatus.put(syncPoint, syncFuture);
    }
  }

  /**
   * Clean up tasks to stop sync point after we have journaled.
   *
   * @param syncPoint the sync point to stop
   * @throws InvalidPathException
   */
  private void stopSyncInternal(AlluxioURI syncPoint) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    // Remove initial sync thread
    Future<?> syncFuture = mSyncPathStatus.remove(syncPoint);
    if (syncFuture != null) {
      syncFuture.cancel(true);
    }

    long mountId = resolution.getMountId();
    if (mFilterMap.containsKey(mountId) && mFilterMap.get(mountId).isEmpty()) {
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
      // syncPoint removed was the last syncPoint for the mountId
      mFilterMap.remove(mountId);
      try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
        ufs.get().stopActiveSyncPolling();
      } catch (IOException e) {
        LOG.warn("Encountered IOException when trying to stop polling thread: {}", e.toString());
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
   * Set the transaction id for a particular mountId.
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
   *
   * This stops four things in the following order.
   * 1. Stop any outstanding initial sync futures for the sync points. (syncFuture.cancel)
   * 2. Stop the heartbeat thread that periodically wakes up to process events that have been
   *    recorded for the past heartbeat interval.
   * 3. Tell the polling thread to stop monitoring the path for events
   * 4. Stop the thread that is polling HDFS for events
   */
  public void stop() {
    if (!mStarted) {
      return;
    }
    mStarted = false;
    for (AlluxioURI syncPoint : mSyncPathList) {
      try {
        stopSyncInternal(syncPoint);
      } catch (InvalidPathException e) {
        LOG.warn("stop: InvalidPathException resolving syncPoint {}, exception {}",
            syncPoint,  e);
        return;
      }
    }
  }

  /**
   * Recover from a stop sync operation.
   *
   * @param uri uri to stop sync
   */
  public void recoverFromStopSync(AlluxioURI uri) {
    if (mSyncPathStatus.containsKey(uri)) {
      // nothing to recover from, since the syncPathStatus still contains syncPoint
      return;
    }
    try {
      // the init sync thread has been removed, to reestablish sync, we need to sync again
      MountTable.Resolution resolution = mMountTable.resolve(uri);
      startInitialFullSync(uri, resolution);
      launchPollingThread(resolution.getMountId(), SyncInfo.INVALID_TXID);
    } catch (Throwable t) {
      LOG.warn("Recovering from stop syncing failed: {}", t.toString());
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
    for (long mountId : new HashSet<>(mFilterMap.keySet())) {
      try {
        // stops sync point under this mount point. Note this clears the sync point and
        // stops associated polling threads.
        stopSyncForMount(mountId);
      } catch (InvalidPathException e) {
        LOG.info("Exception resetting mountId {}, exception: {}", mountId, e);
      }
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.ACTIVE_SYNC_MANAGER;
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(
        Iterators.concat(getSyncPathIterator(), getTxIdIterator()));
  }
}

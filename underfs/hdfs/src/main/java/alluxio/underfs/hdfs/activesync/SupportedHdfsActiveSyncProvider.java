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

package alluxio.underfs.hdfs.activesync;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.resource.LockResource;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsActiveSyncProvider;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Supported Hdfs Active Sync Provider.
 */
public class SupportedHdfsActiveSyncProvider implements HdfsActiveSyncProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SupportedHdfsActiveSyncProvider.class);
  private final HdfsAdmin mHdfsAdmin;
  // These read write locks protect the state (maps) managed by this class.
  private final Lock mReadLock;
  private final Lock mWriteLock;
  private final ExecutorService mExecutorService;
  private final int mActiveUfsSyncMaxActivity;
  private final int mActiveUfsSyncMaxAge;
  private final long mActiveUfsPollTimeoutMs;
  private final long mActiveUfsSyncEventRateInterval;
  private Future<?> mPollingThread;
  private List<AlluxioURI> mUfsUriList;

  // a map mapping SyncPoints to a set of files that have been changed under that syncPoint
  private Map<String, Set<AlluxioURI>> mChangedFiles;
  // Use an integer to indicate the activity level of the sync point
  // TODO(yuzhu): Merge the three maps into one map
  private Map<String, Integer> mActivity;
  private Map<String, Integer> mAge;
  private Map<String, Long> mTxIdMap;
  private long mCurrentTxId;
  private boolean mEventMissed;

  /**
   * Constructor for supported Hdfs Active Sync Provider.
   *
   * @param uri the hdfs uri
   * @param conf the hdfs conf
   * @param ufsConf Alluxio UFS configuration
   */
  public SupportedHdfsActiveSyncProvider(URI uri, org.apache.hadoop.conf.Configuration conf,
      UnderFileSystemConfiguration ufsConf)
      throws IOException {
    mHdfsAdmin = new HdfsAdmin(uri, conf);
    mChangedFiles = new ConcurrentHashMap<>();
    mActivity = new ConcurrentHashMap<>();
    mAge = new ConcurrentHashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    mReadLock = lock.readLock();
    mWriteLock = lock.writeLock();
    mExecutorService = Executors
        .newFixedThreadPool(ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_THREAD_POOL_SIZE),
            ThreadFactoryUtils.build("SupportedHdfsActiveSyncProvider-%d", true));
    mPollingThread = null;
    mUfsUriList = new CopyOnWriteArrayList<>();
    mEventMissed = false;
    mTxIdMap = new ConcurrentHashMap<>();
    mCurrentTxId = SyncInfo.INVALID_TXID;
    mActiveUfsSyncMaxActivity = ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_MAX_ACTIVITIES);
    mActiveUfsSyncMaxAge = ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_MAX_AGE);
    mActiveUfsPollTimeoutMs = ufsConf.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_POLL_TIMEOUT);
    mActiveUfsSyncEventRateInterval =
        ufsConf.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_EVENT_RATE_INTERVAL);
  }

  /**
   * Start the accounting for the next window of events.
   *
   * This includes increasing the age of the unsynced syncpoints and decrease the activity of
   * unsynced syncpoints.
   */
  private void initNextWindow() {
    for (String syncPoint : mActivity.keySet()) {
      mActivity.put(syncPoint, mActivity.get(syncPoint).intValue() / 10);
      mAge.put(syncPoint, mAge.get(syncPoint).intValue() + 1);
    }
  }

  private void recordFileChanged(String syncPoint, String filePath, long txId) {
    AlluxioURI syncPointUri = new AlluxioURI(syncPoint);

    try (LockResource r = new LockResource(mWriteLock)) {
      if (!mChangedFiles.containsKey(syncPoint)) {
        mChangedFiles.put(syncPoint, new ConcurrentHashSet<>());
        mActivity.put(syncPoint, 0);
        mAge.put(syncPoint, 0);
        mTxIdMap.put(syncPoint, txId);
      }
      mChangedFiles.get(syncPoint).add(
          new AlluxioURI(syncPointUri.getScheme(), syncPointUri.getAuthority(), filePath));
      mActivity.put(syncPoint, mActivity.get(syncPoint).intValue() + 1);
    }
  }

  private void syncSyncPoint(String syncPoint) {
    mChangedFiles.remove(syncPoint);
    mActivity.remove(syncPoint);
    mAge.remove(syncPoint);
    mTxIdMap.remove(syncPoint);
  }

  private boolean processEvent(Event event, List<AlluxioURI> syncPointList, long txId) {
    boolean fileMatch = false;
    String filePath = "";
    String renameFilePath = "";

    switch (event.getEventType()) {
      case CREATE:
        Event.CreateEvent createEvent = (Event.CreateEvent) event;
        filePath = createEvent.getPath();
        break;
      case UNLINK:
        Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
        filePath = unlinkEvent.getPath();
        break;
      case APPEND:
        Event.AppendEvent appendEvent = (Event.AppendEvent) event;
        filePath = appendEvent.getPath();
        break;
      case RENAME:
        Event.RenameEvent renameEvent = (Event.RenameEvent) event;
        filePath = renameEvent.getSrcPath();
        renameFilePath = renameEvent.getDstPath();
        break;
      case METADATA:
        Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
        filePath = metadataUpdateEvent.getPath();
        break;
      default:
        break;
    }
    if (filePath.isEmpty()) {
      return false;
    }
    for (AlluxioURI syncPoint :  syncPointList) {
      try {
        // find out if the changed file falls under one of the sync points
        if (PathUtils.hasPrefix(filePath, syncPoint.getPath())) {
          fileMatch = true;
          recordFileChanged(syncPoint.toString(), filePath, txId);
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", filePath);
      }

      try {
        // find out if the changed file falls under one of the sync points
        if ((!renameFilePath.isEmpty())
            && PathUtils.hasPrefix(renameFilePath, syncPoint.getPath())) {
          fileMatch = true;
          recordFileChanged(syncPoint.toString(), renameFilePath, txId);
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", renameFilePath);
      }
    }
    try (LockResource r = new LockResource(mWriteLock)) {
      mCurrentTxId = txId;
    }
    return fileMatch;
  }

  @Override
  public boolean startPolling(long txId) throws IOException {
    if (mPollingThread == null) {
      DFSInotifyEventInputStream eventStream;
      LOG.info("Start polling from event txID {}", txId);
      if (txId != SyncInfo.INVALID_TXID) {
        eventStream = mHdfsAdmin.getInotifyEventStream(txId);
      } else {
        eventStream = mHdfsAdmin.getInotifyEventStream();
      }
      mPollingThread = mExecutorService.submit(() -> {
        pollEvent(eventStream);
      });
      return true;
    }
    return false;
  }

  @Override
  public boolean stopPolling() {
    if (mPollingThread != null) {
      mPollingThread.cancel(true);
      mPollingThread = null;
      return true;
    }
    return false;
  }

  /**
   * startSync on a ufs uri.
   *
   * @param ufsUri the ufs uri to monitor for sync
   */
  @Override
  public void startSync(AlluxioURI ufsUri) {
    LOG.debug("Add {} as a sync point", ufsUri.toString());
    mUfsUriList.add(ufsUri);
  }

  /**
   * stop sync on a ufs uri.
   *
   * @param ufsUri the ufs uri to stop monitoring for sync
   */
  @Override
  public void stopSync(AlluxioURI ufsUri) {
    LOG.debug("attempt to remove {} from sync point list", ufsUri.toString());
    mUfsUriList.remove(ufsUri);
  }

  /**
   * Fetch and process events.
   * @param eventStream event stream
   */
  public void pollEvent(DFSInotifyEventInputStream eventStream) {
    EventBatch batch;
    LOG.debug("Polling thread starting, with timeout {} ms", mActiveUfsPollTimeoutMs);
    int count = 0;
    long start = System.currentTimeMillis();

    long behind = eventStream.getTxidsBehindEstimate();

    while (!Thread.currentThread().isInterrupted()) {
      try {
        batch = eventStream.poll(mActiveUfsPollTimeoutMs, TimeUnit.MILLISECONDS);

        if (batch != null) {
          long txId = batch.getTxid();
          count++;
          for (Event event : batch.getEvents()) {
            processEvent(event, mUfsUriList, txId);
          }
        }
        long end = System.currentTimeMillis();
        if (end > (start + mActiveUfsSyncEventRateInterval)) {
          long currentlyBehind = eventStream.getTxidsBehindEstimate();
          LOG.info("HDFS generated {} events in {} ms, at a rate of {} rps",
              count + currentlyBehind - behind ,
              end - start,
              String.format("%.2f", (count + currentlyBehind - behind) * 1000.0 / (end - start)));
          LOG.info("processed {} events in {} ms, at a rate of {} rps", count,
              end - start,
              String.format("%.2f", count * 1000.0 / (end - start)));
          LOG.info("Currently TxidsBehindEstimate by {}", currentlyBehind);
          behind = currentlyBehind;
          start = end;
          count = 0;
        }
      } catch (IOException e) {
        LOG.warn("IOException occured during polling inotify {}", e);
        if (e.getCause() instanceof InterruptedException) {
          return;
        }
      } catch (MissingEventsException e) {
        LOG.warn("MissingEventException during polling {}", e);
        mEventMissed = true;
        // need to sync all syncpoints at this point
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException during polling {}", e);
        return;
      }
    }
  }

  private long getLastTxId() {
    if (mTxIdMap.isEmpty()) {
      return mCurrentTxId;
    } else {
      return Collections.min(mTxIdMap.values());
    }
  }

  /**
   * Get the activity sync info.
   *
   * @return SyncInfo object which encapsulates the necessary information about changes
   */
  public SyncInfo getActivitySyncInfo() {
    // The overview of this method is
    // 1. setup a source of event
    // 2. Filter based on the paths associated with this mountId
    // 3. Build History for each of the syncPoint
    // 4. If heurstics function returns sync, then we sync the syncPoint

    if (mPollingThread == null) {
      return SyncInfo.emptyInfo();
    }

    Map<AlluxioURI, Set<AlluxioURI>> syncPointFiles = new HashMap<>();
    long txId = 0;
    try (LockResource r = new LockResource(mWriteLock)) {
      initNextWindow();
      if (mEventMissed) {
        // force sync every syncpoint
        for (AlluxioURI uri : mUfsUriList) {
          syncPointFiles.put(uri, null);
          syncSyncPoint(uri.toString());
        }
        mEventMissed = false;
        LOG.debug("Missed event, syncing all sync points\n{}",
            Arrays.toString(syncPointFiles.keySet().toArray()));
        SyncInfo syncInfo = new SyncInfo(syncPointFiles, true, getLastTxId());
        return syncInfo;
      }
      for (String syncPoint : mActivity.keySet()) {
        AlluxioURI syncPointURI = new AlluxioURI(syncPoint);
        // if the activity level is below the threshold or the sync point is too old, we sync
        if (mActivity.get(syncPoint) < mActiveUfsSyncMaxActivity
            || mAge.get(syncPoint) > mActiveUfsSyncMaxAge) {
          if (!syncPointFiles.containsKey(syncPointURI)) {
            syncPointFiles.put(syncPointURI, mChangedFiles.get(syncPoint));
          }
          syncSyncPoint(syncPoint);
        }
      }
      txId = getLastTxId();
    }
    LOG.debug("Syncing {} files", syncPointFiles.size());
    LOG.debug("Last transaction id {}", txId);

    SyncInfo syncInfo = new SyncInfo(syncPointFiles, false, txId);
    return syncInfo;
  }
}

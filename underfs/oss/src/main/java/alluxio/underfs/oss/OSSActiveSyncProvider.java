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

package alluxio.underfs.oss;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.resource.LockResource;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.logging.SamplingLogger;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * OSS Active Sync Provider.
 */
public class OSSActiveSyncProvider {
  private static final Logger LOG = LoggerFactory.getLogger(OSSActiveSyncProvider.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);

  // These read write locks protect the state (maps) managed by this class.
  private final Lock mReadLock;
  private final Lock mWriteLock;
  private final ScheduledExecutorService mExecutorService;
  private final int mActiveUfsSyncMaxActivity;
  private final int mActiveUfsSyncMaxAge;

  private final long mActiveUfsSyncInterval;
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

  private final OSS mClient;

  private ConcurrentMap<AlluxioURI, Long> mUfsUriLastModified;

  private final String mOssFolderSuffix = "_$folder$";

  /**
   * Constructor for supported OSS Active Sync Provider.
   *
   * @param ufsConf   Alluxio UFS configuration
   * @param ossClient The OSS Client
   */
  public OSSActiveSyncProvider(UnderFileSystemConfiguration ufsConf, OSS ossClient) {
    mChangedFiles = new ConcurrentHashMap<>();
    mActivity = new ConcurrentHashMap<>();
    mAge = new ConcurrentHashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    mReadLock = lock.readLock();
    mWriteLock = lock.writeLock();
    mExecutorService = Executors.newScheduledThreadPool(
        ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_THREAD_POOL_SIZE),
        ThreadFactoryUtils.build("OSSActiveSyncProvider-%d", false));
    mPollingThread = null;
    mUfsUriList = new CopyOnWriteArrayList<>();
    mTxIdMap = new ConcurrentHashMap<>();
    mCurrentTxId = SyncInfo.INVALID_TXID;
    mActiveUfsSyncMaxActivity = ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_MAX_ACTIVITIES);
    mActiveUfsSyncMaxAge = ufsConf.getInt(PropertyKey.MASTER_UFS_ACTIVE_SYNC_MAX_AGE);
    mActiveUfsSyncInterval = ufsConf.getMs(PropertyKey.MASTER_UFS_ACTIVE_SYNC_INTERVAL);
    mClient = ossClient;
    mUfsUriLastModified = new ConcurrentHashMap<>();
  }

  /**
   * Start the accounting for the next window of events.
   * <p>
   * This includes increasing the age of the unsynced syncpoints and decrease the activity of
   * unsynced syncpoints.
   */
  private void initNextWindow() {
    for (Map.Entry<String, Integer> activity : mActivity.entrySet()) {
      String syncPoint = activity.getKey();
      mActivity.put(syncPoint, activity.getValue() / 10);
      mAge.put(syncPoint, mAge.get(syncPoint) + 1);
    }
  }

  private void recordFileChanged(String syncPoint, String filePath, long txId) {
    AlluxioURI syncPointUri = new AlluxioURI(syncPoint);

    mChangedFiles.computeIfAbsent(syncPoint, (key) -> {
      mActivity.put(syncPoint, 0);
      mAge.put(syncPoint, 0);
      mTxIdMap.put(syncPoint, txId);
      return new ConcurrentHashSet<>();
    });
    try (LockResource r = new LockResource(mWriteLock)) {
      mChangedFiles.get(syncPoint).add(
          new AlluxioURI(syncPointUri.getRootPath() + filePath));
      mActivity.put(syncPoint, mActivity.get(syncPoint) + 1);
    }
  }

  private void syncSyncPoint(String syncPoint) {
    mChangedFiles.remove(syncPoint);
    mActivity.remove(syncPoint);
    mAge.remove(syncPoint);
    mTxIdMap.remove(syncPoint);
  }

  private void updateChangedFiles() {
    for (AlluxioURI ufsUri : mUfsUriList) {
      List<AlluxioURI> changedFiles = getChangedFiles(ufsUri);
      for (AlluxioURI changedFile : changedFiles) {
        recordFileChanged(ufsUri.toString(), changedFile.toString(), mCurrentTxId);
      }
    }
  }

  private void removeUfsUriLastModified(AlluxioURI ufsUri) {
    try {
      if (isDirectory(ufsUri)) {
        String bucketName = UnderFileSystemUtils.getBucketName(ufsUri);
        String prefix = cutSlash(ufsUri.getPath()) + "/";
        ObjectListing objectListing = mClient.listObjects(bucketName, prefix);
        List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
        for (OSSObjectSummary s : sums) {
          AlluxioURI fileUri = new AlluxioURI(s.getKey());
          mUfsUriLastModified.remove(fileUri);
        }
      } else {
        String bucketName = UnderFileSystemUtils.getBucketName(ufsUri);
        String fileName = cutSlash(ufsUri.getPath());
        if (mClient.doesObjectExist(bucketName, fileName)) {
          OSSObject ossObject = mClient.getObject(bucketName, ufsUri.getPath());
          if (null != ossObject) {
            mUfsUriLastModified.remove(ufsUri);
          }
        }
      }
    } catch (OSSException oe) {
      LOG.info("Caught an OSSException, which means your request made it to OSS, "
          + "but was rejected with an error response for some reason.");
      LOG.info("Error Message:" + oe.getErrorMessage());
      LOG.info("Error Code:" + oe.getErrorCode());
      LOG.info("Request ID:" + oe.getRequestId());
      LOG.info("Host ID:" + oe.getHostId());
    } catch (ClientException ce) {
      LOG.info("Caught an ClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with OSS, "
          + "such as not being able to access the network.");
      LOG.info("Error Message:" + ce.getMessage());
    }
  }

  private List<AlluxioURI> getChangedFiles(AlluxioURI ufsUri) {
    try {
      if (isDirectory(ufsUri)) {
        List<AlluxioURI> filesUri = new ArrayList<>();
        String bucketName = UnderFileSystemUtils.getBucketName(ufsUri);
        String prefix = cutSlash(ufsUri.getPath()) + "/";
        ObjectListing objectListing = mClient.listObjects(bucketName, prefix);
        List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
        for (OSSObjectSummary s : sums) {
          AlluxioURI fileUri = new AlluxioURI(s.getKey());
          if (isChanged(fileUri, s.getLastModified().getTime())) {
            updateUriLastModified(fileUri, s.getLastModified().getTime());
            filesUri.add(new AlluxioURI(s.getKey()));
            LOG.info("\t" + s.getKey() + " is changed. ");
          }
        }
        return filesUri;
      } else {
        List<AlluxioURI> filesUri = new ArrayList<>();
        String bucketName = UnderFileSystemUtils.getBucketName(ufsUri);
        String fileName = cutSlash(ufsUri.getPath());
        if (mClient.doesObjectExist(bucketName, fileName)) {
          OSSObject ossObject = mClient.getObject(bucketName, ufsUri.getPath());
          if (null != ossObject
              && isChanged(ufsUri, ossObject.getObjectMetadata().getLastModified().getTime())) {
            updateUriLastModified(ufsUri,
                ossObject.getObjectMetadata().getLastModified().getTime());
            filesUri.add(new AlluxioURI(ossObject.getKey()));
            LOG.info("\t" + ossObject.getKey() + " is changed. ");
          }
        }
        return filesUri;
      }
    } catch (OSSException oe) {
      LOG.info("Caught an OSSException, which means your request made it to OSS, "
          + "but was rejected with an error response for some reason.");
      LOG.info("Error Message:" + oe.getErrorMessage());
      LOG.info("Error Code:" + oe.getErrorCode());
      LOG.info("Request ID:" + oe.getRequestId());
      LOG.info("Host ID:" + oe.getHostId());
      return Collections.emptyList();
    } catch (ClientException ce) {
      LOG.info("Caught an ClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with OSS, "
          + "such as not being able to access the network.");
      LOG.info("Error Message:" + ce.getMessage());
      return Collections.emptyList();
    }
  }

  private boolean isDirectory(AlluxioURI ufsUri) {
    String bucketName = UnderFileSystemUtils.getBucketName(ufsUri);
    String fileName = ufsUri.getPath();
    fileName = cutSlash(fileName);
    if (mClient.doesObjectExist(bucketName, fileName + mOssFolderSuffix)) {
      return true;
    }
    return false;
  }

  private String cutSlash(String fileName) {
    String newFileName = fileName;
    if (newFileName.startsWith("/")) {
      newFileName = newFileName.substring(1);
    }
    if (newFileName.endsWith("/")) {
      newFileName = newFileName.substring(0, newFileName.length() - 1);
    }
    return newFileName;
  }

  private boolean isChanged(AlluxioURI ufsUri, long ufsLastModifiedTimestamp) {
    Long lastModified = mUfsUriLastModified.get(ufsUri);
    if (null == lastModified || lastModified < ufsLastModifiedTimestamp) {
      return true;
    }
    return false;
  }

  private void updateUriLastModified(AlluxioURI ufsUri, long ufsLastModifiedTimestamp) {
    mUfsUriLastModified.put(ufsUri, ufsLastModifiedTimestamp);
  }

  /**
   * start polling thread.
   *
   * @param txId transaction id to start monitoring
   * @return true if polling thread started successfully
   */
  public boolean startPolling(long txId) throws IOException {
    if (mPollingThread == null) {
      LOG.info("Start polling from event txID {}", txId);
      mPollingThread = mExecutorService.scheduleAtFixedRate(() -> updateChangedFiles(), 0,
          mActiveUfsSyncInterval, TimeUnit.MILLISECONDS);
      return true;
    }
    return false;
  }

  /**
   * stop polling thread.
   *
   * @return true if polling thread stopped successfully
   */
  public boolean stopPolling() {
    if (mPollingThread != null) {
      mPollingThread.cancel(true);
      mPollingThread = null;
      mUfsUriLastModified.clear();
      return true;
    }
    return false;
  }

  /**
   * startSync on a ufs uri.
   *
   * @param ufsUri the ufs uri to monitor for sync
   */
  public void startSync(AlluxioURI ufsUri) {
    LOG.debug("Add {} as a sync point", ufsUri);
    mUfsUriList.add(ufsUri);
  }

  /**
   * stop sync on a ufs uri.
   *
   * @param ufsUri the ufs uri to stop monitoring for sync
   */
  public void stopSync(AlluxioURI ufsUri) {
    LOG.debug("attempt to remove {} from sync point list", ufsUri);
    mUfsUriList.remove(ufsUri);
    removeUfsUriLastModified(ufsUri);
  }

  /**
   * get the last transaction id.
   *
   * @return the last transaction id
   */
  private long getLastTxId() {
    if (mTxIdMap.isEmpty()) {
      return mCurrentTxId;
    } else {
      return Collections.min(mTxIdMap.values());
    }
  }

  /**
   * Get Active SyncInfo from the sync provider.
   *
   * @return SyncInfo a syncInfo containing information about what to sync and how to sync
   */
  public SyncInfo getActivitySyncInfo() {
    // The overview of this method is
    // 1. setup a source of event
    // 2. Filter based on the paths associated with this mountId
    // 3. Build History for each of the syncPoint
    // 4. If heuristics function returns sync, then we sync the syncPoint

    if (mPollingThread == null) {
      return SyncInfo.emptyInfo();
    }

    Map<AlluxioURI, Set<AlluxioURI>> syncPointFiles = new HashMap<>();
    long txId = 0;
    try (LockResource r = new LockResource(mWriteLock)) {
      initNextWindow();
      for (Map.Entry<String, Integer> activity : mActivity.entrySet()) {
        String syncPoint = activity.getKey();
        AlluxioURI syncPointURI = new AlluxioURI(syncPoint);
        // if the activity level is below the threshold or the sync point is too old, we sync
        if (activity.getValue() < mActiveUfsSyncMaxActivity
            || mAge.get(syncPoint) > mActiveUfsSyncMaxAge) {
          if (!syncPointFiles.containsKey(syncPointURI)) {
            syncPointFiles.put(syncPointURI, mChangedFiles.get(syncPoint));
          }
          syncSyncPoint(syncPoint);
        }
      }
      txId = getLastTxId();
    }
    LOG.debug("Syncing {} files with last transaction id {}", syncPointFiles.size(), txId);

    SyncInfo syncInfo = new SyncInfo(syncPointFiles, false, txId);
    return syncInfo;
  }
}

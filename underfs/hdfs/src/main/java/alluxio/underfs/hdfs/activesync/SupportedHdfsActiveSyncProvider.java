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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.SyncInfo;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.InvalidPathException;
import alluxio.underfs.hdfs.HdfsActiveSyncProvider;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supported Hdfs Active Sync Provider.
 */
public class SupportedHdfsActiveSyncProvider implements HdfsActiveSyncProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SupportedHdfsActiveSyncProvider.class);
  private final HdfsAdmin mHdfsAdmin;
  private static final int MAX_ACTIVITY =
      Configuration.getInt(PropertyKey.MASTER_ACTIVE_UFS_SYNC_MAX_ACTIVITY);
  private static final int MAX_AGE =
      Configuration.getInt(PropertyKey.MASTER_ACTIVE_UFS_SYNC_MAX_AGE);

  // a map mapping SyncPoints to a set of files that have been changed under that syncPoint
  private Map<String, Set<AlluxioURI>> mChangedFiles;
  // Use an integer to indicate the activity level of the sync point
  private Map<String, Integer> mActivity;
  private Map<String, Integer> mAge;
  private DFSInotifyEventInputStream mEventStream;

  /**
   * Constructor for supported Hdfs Active Sync Provider.
   *
   * @param uri the hdfs uri
   * @param conf the hdfs conf
   */
  public SupportedHdfsActiveSyncProvider(URI uri, org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    mHdfsAdmin = new HdfsAdmin(uri, conf);
    mChangedFiles = new ConcurrentHashMap<>();
    mActivity = new ConcurrentHashMap<>();
    mAge = new ConcurrentHashMap<>();
    mEventStream = mHdfsAdmin.getInotifyEventStream();
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

  private void addFile(String syncPoint, String filePath) {
    LOG.info("add file for syncPoint {}, at path {}", syncPoint, filePath);
    AlluxioURI syncPointUri = new AlluxioURI(syncPoint);
    if (!mChangedFiles.containsKey(syncPoint)) {
      mChangedFiles.put(syncPoint, new ConcurrentHashSet<>());
      mActivity.put(syncPoint, 0);
      mAge.put(syncPoint, 0);
    }
    mChangedFiles.get(syncPoint).add(
        new AlluxioURI(syncPointUri.getScheme(), syncPointUri.getAuthority(), filePath));
    mActivity.put(syncPoint, mActivity.get(syncPoint).intValue() + 1);
  }

  private void clearFile(String syncPoint) {
    LOG.info("clear file for syncPoint {}, ", syncPoint);
    mChangedFiles.remove(syncPoint);
    mActivity.remove(syncPoint);
    mAge.remove(syncPoint);
  }

  private boolean processEvent(Event event, List<AlluxioURI> syncPointList) {
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
        LOG.debug("file path in processEvent" + filePath);
        LOG.debug("syncPoint in processEvent" + syncPoint.getPath());
        // find out if the changed file falls under one of the sync points
        if (PathUtils.hasPrefix(filePath, syncPoint.getPath())) {
          fileMatch = true;
          addFile(syncPoint.toString(), filePath);
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", filePath);
      }

      try {
        // find out if the changed file falls under one of the sync points
        if ((!renameFilePath.isEmpty()) && PathUtils.hasPrefix(renameFilePath, syncPoint.getPath())) {
          fileMatch = true;
          addFile(syncPoint.toString(), renameFilePath);
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", renameFilePath);
      }
    }
    return fileMatch;
  }

  /**
   * Get the activity sync info.
   *
   * @param syncPointList List of alluxio URIs to watch for changes
   * @return SyncInfo object which encapsulates the necessary information about changes
   */
  public SyncInfo getActivitySyncInfo(List<AlluxioURI> syncPointList) {
    // The overview of this method is
    // 1. setup a source of event
    // 2. Filter based on the paths associated with this mountId
    // 3. Build History for each of the syncPoint
    // 4. If heurstics function returns sync, then we sync the syncPoint

    if (mEventStream == null) {
      return null;
    }

    LOG.debug("Activity map");
    LOG.debug(Arrays.toString(mActivity.entrySet().toArray()));
    LOG.debug("Age map");
    LOG.debug(Arrays.toString(mAge.entrySet().toArray()));

    LOG.debug("syncPointList");
    LOG.debug(Arrays.toString(syncPointList.toArray()));
    initNextWindow();
    EventBatch batch;
    try {
      while ((batch = mEventStream.poll()) != null) {
        if (batch == null) {
          break;
        } else {
          LOG.info("received events");
          Arrays.stream(batch.getEvents())
              .parallel().forEach(event -> processEvent(event, syncPointList));
        }
      }
    } catch (IOException e) {
      LOG.warn("IOException occured during polling inotify", e);
    } catch (MissingEventsException e) {
      LOG.warn("MissingEventException {}", e);
    }

    Map<AlluxioURI, Set<AlluxioURI>> syncPointFiles = new ConcurrentHashMap<>();
    for (String syncPoint : mActivity.keySet()) {
      AlluxioURI syncPointURI = new AlluxioURI(syncPoint);
      // if the activity level is below the threshold or the sync point is too old, we sync
      if (mActivity.get(syncPoint) < MAX_ACTIVITY || mAge.get(syncPoint) > MAX_AGE) {
        if (!syncPointFiles.containsKey(syncPointURI)) {
          syncPointFiles.put(syncPointURI, mChangedFiles.get(syncPoint));
        }
        clearFile(syncPoint);
      }
    }
    LOG.debug("syncPointFiles");
    LOG.debug(Arrays.toString(syncPointFiles.entrySet().toArray()));

    SyncInfo syncInfo = new SyncInfo(syncPointFiles);
    return syncInfo;

  }
}

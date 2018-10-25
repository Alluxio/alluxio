package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HdfsActiveSyncProvider {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsActiveSyncProvider.class);
  private final HdfsAdmin mHdfsAdmin;
  private static final int MAX_ACTIVITY = 10;
  private static final int MAX_AGE = 10;

  // a map mapping SyncPoints to a set of files that have been changed under that syncPoint
  private Map<String, Set<AlluxioURI>> mChangedFiles;
  // Use an integer to indicate the activity level of the sync point
  private Map<String, Integer> mActivity;
  private Map<String, Integer> mAge;

  public HdfsActiveSyncProvider(HdfsAdmin hdfsAdmin) {
    mHdfsAdmin = hdfsAdmin;
    mChangedFiles = new ConcurrentHashMap<>();
    mActivity = new ConcurrentHashMap<>();
  }

  private void initNextWindow() {
    for (String syncPoint : mActivity.keySet()) {
      mActivity.put(syncPoint, new Integer(mActivity.get(syncPoint).intValue() / 10));
      mAge.put(syncPoint, new Integer(mAge.get(syncPoint).intValue() + 1));
    }
  }

  private void addFile(String syncPoint, String filePath) {
    if (!mChangedFiles.containsKey(syncPoint)){
      mChangedFiles.put(syncPoint, new ConcurrentHashSet<>());
      mActivity.put(syncPoint, new Integer(0));
      mAge.put(syncPoint, new Integer(0));
    }
    mChangedFiles.get(syncPoint).add(new AlluxioURI(filePath));
    mActivity.put(syncPoint, new Integer(mActivity.get(syncPoint).intValue() + 1));
  }

  private void clearFile(String syncPoint){
    mChangedFiles.remove(syncPoint);
    mActivity.remove(syncPoint);
    mAge.remove(syncPoint);
  }

  private boolean isEventModification(Event inotifyEvent) {
    List<Event.EventType> eventList = Arrays.asList(new Event.EventType[]{Event.EventType.APPEND,
        Event.EventType.CREATE, Event.EventType.METADATA, Event.EventType.RENAME,
        Event.EventType.TRUNCATE, Event.EventType.UNLINK});
    return eventList.contains(inotifyEvent.getEventType());
  }

  public boolean filterEvent(Event event, List<AlluxioURI> syncPointList) {
    boolean isModified = isEventModification(event);
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
      default:
        break;
    }
    for (AlluxioURI syncPoint :  syncPointList) {
      try {
        // find out if the changed file falls under one of the sync points
        if (PathUtils.hasPrefix(filePath, syncPoint.getPath())) {
          fileMatch = true;
          addFile(syncPoint.getPath(), filePath);
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", filePath);
      }

      try {
        // find out if the changed file falls under one of the sync points
        if ((!renameFilePath.equals("")) && PathUtils.hasPrefix(renameFilePath, syncPoint.getPath())) {
          fileMatch = true;
          addFile(syncPoint.getPath(), renameFilePath);
          break;
        }
      } catch (InvalidPathException e) {
        LOG.info("Invalid path encountered {} ", renameFilePath);
      }
    }

    return isModified && fileMatch;
  }

  public SyncInfo getActivitySyncInfo(List<AlluxioURI> syncPointList) {
    // The overview of this method is
    // 1. setup a source of event
    // 2. Filter based on the paths associated with this mountId
    // 3. Build History for each of the syncPoint
    // 4. If heurstics function returns sync, then we sync the syncPoint

    if (mHdfsAdmin == null) {
      return null;
    }
    try {
      DFSInotifyEventInputStream eventStream = mHdfsAdmin.getInotifyEventStream();

      EventBatch batch = eventStream.take();
      initNextWindow();
      Arrays.stream(batch.getEvents())
          .parallel().forEach(event -> filterEvent(event, syncPointList));

      List<AlluxioURI> pathsToBeSynced = new ArrayList<>();
      Map<AlluxioURI, Set<AlluxioURI>> syncPointFiles = new ConcurrentHashMap<>();
      for (String syncPoint : mActivity.keySet()) {
        AlluxioURI syncPointURI = new AlluxioURI(syncPoint);
        // if the activity level is below the threshold or the sync point is too old, we sync
        if (mActivity.get(syncPoint) < MAX_ACTIVITY || mAge.get(syncPoint) > MAX_AGE) {
          pathsToBeSynced.add(new AlluxioURI(syncPoint));
          if (!syncPointFiles.containsKey(syncPointURI)) {
            syncPointFiles.put(syncPointURI, mChangedFiles.get(syncPoint));
          }
          clearFile(syncPoint);
        }
      }
      SyncInfo syncInfo = new SyncInfo(pathsToBeSynced, syncPointFiles);
      return syncInfo;

    } catch (IOException e) {
      LOG.warn("IOException occured during polling inotify", e);
    } catch (Exception e) {

    }
    return null;
  }
}

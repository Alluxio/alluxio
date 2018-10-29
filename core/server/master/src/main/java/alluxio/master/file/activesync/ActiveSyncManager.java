package alluxio.master.file.activesync;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ActiveSyncManager {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncManager.class);
  private final MountTable mMountTable;

  private final List<AlluxioURI> mSyncPathList;
  // a map which maps each UFS rootPath to a thread polling that UFS
  private final Map<String, Future<?>> mPollerMap;
  //  a map which maps each UFS rootPath to a list of paths being actively synced on that thread
  private final Map<String, List<AlluxioURI>> mFilterMap;

  private FileSystemMaster mFileSystemMaster;

  public ActiveSyncManager(MountTable mountTable,
      FileSystemMaster fileSystemMaster) {
    mMountTable = mountTable;
    mPollerMap = new ConcurrentHashMap<>();
    mFilterMap = new ConcurrentHashMap<>();
    mSyncPathList = new CopyOnWriteArrayList<>();
    mFileSystemMaster = fileSystemMaster;
  }

  public boolean isActivelySynced(AlluxioURI path) throws InvalidPathException {
    for (AlluxioURI syncedPath : mSyncPathList) {
      if (PathUtils.hasPrefix(path.getPath(), syncedPath.getPath())) {
        return true;
      }
    }
    return false;
  }

  public boolean addSyncPoint(AlluxioURI syncPoint) throws InvalidPathException {
    LOG.info("adding syncPoint {}", syncPoint.getPath());
    if (!isActivelySynced(syncPoint)) {
      MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
      AlluxioURI ufsUri = resolution.getUri();
      String rootPath = ufsUri.getRootPath();
      LOG.info("rootPath {}", rootPath);
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        if (!ufsResource.get().supportsActiveSync()) {
          throw new UnsupportedOperationException("Active Syncing is not supported on this UFS type"
              + ufsResource.get().getUnderFSType());
        }
        if (!mPollerMap.containsKey(rootPath)) {
          ActiveSyncer syncer = new ActiveSyncer(mFileSystemMaster, this, mMountTable, rootPath);

          Future<?> future = mFileSystemMaster.getExecutorService().submit(
              new HeartbeatThread(HeartbeatContext.MASTER_ACTIVE_SYNC,
                  syncer,
                  (int) Configuration.getMs(PropertyKey.MASTER_ACTIVE_UFS_SYNC_INTERVAL_MS)));
          mPollerMap.put(rootPath, future);
        }

        // Add the new sync point to the filter map
        if (mFilterMap.containsKey(rootPath)) {
          mFilterMap.get(rootPath).add(syncPoint);
        } else {
          mFilterMap.put(rootPath, new CopyOnWriteArrayList<>(Arrays.asList(syncPoint)));
        }
        // Add to the sync point list
        mSyncPathList.add(syncPoint);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean removeSyncPoint(AlluxioURI syncPoint) throws InvalidPathException {
    if (!mSyncPathList.contains(syncPoint)) {
      return false;
    }
    MountTable.Resolution resolution = mMountTable.resolve(syncPoint);
    AlluxioURI ufsUri = resolution.getUri();
    String rootPath = ufsUri.getRootPath();

    if (mFilterMap.containsKey(rootPath)) {
      mFilterMap.get(rootPath).remove(syncPoint);
      if (mFilterMap.get(rootPath).isEmpty()) {
        // syncPoint removed was the last syncPoint for the rootPath
        mFilterMap.remove(rootPath);
        Future<?> future = mPollerMap.remove(rootPath);
        future.cancel(true);
      }
      mSyncPathList.remove(syncPoint);
    } else {
      mSyncPathList.remove(syncPoint);
      // We should not be in this situation
      throw new RuntimeException(String.format("rootPath for the syncPoint %s not found", syncPoint.toString()));
    }
    return true;
  }

  public List<AlluxioURI> getFilterList(String rootPath) {
    return mFilterMap.get(rootPath);
  }

  public List<String> getSyncPathList() {
    return mSyncPathList.stream()
        .map(AlluxioURI::getPath)
        .collect(Collectors.toList());
  }
}

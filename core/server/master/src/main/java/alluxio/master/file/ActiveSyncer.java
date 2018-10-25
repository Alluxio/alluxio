package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.activesync.ActiveSyncManager;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import org.omg.CORBA.DynAnyPackage.Invalid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Periodically sync the files for a particular mountpoint
 */
@NotThreadSafe
public class ActiveSyncer implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveSyncer.class);
  private final FileSystemMaster mFileSystemMaster;
  private final ActiveSyncManager mSyncManager;
  private final MountTable mMountTable;
  private final String mRootPath;


  /**
   * Constructs a new {@link ActiveSyncer}.
   */
  public ActiveSyncer(FileSystemMaster fileSystemMaster, ActiveSyncManager syncManager,
      MountTable mountTable, String rootPath) {
    mFileSystemMaster = fileSystemMaster;
    mSyncManager = syncManager;
    mRootPath = rootPath;
    mMountTable = mountTable;
  }


  @Override
  public void heartbeat() {
    List<AlluxioURI> filterList =  mSyncManager.getFilterList(mRootPath);
    List<AlluxioURI> ufsUriList = filterList.stream().map(alluxioURI -> {
      try {
        return mMountTable.resolve(alluxioURI).getUri();
      } catch (InvalidPathException e) {
        LOG.warn("Invalid path " + alluxioURI.getPath());
        return null;
      }
    }).collect(Collectors.toList());
    if (filterList == null || filterList.isEmpty()) {
      return;
    }

    AlluxioURI path = filterList.get(0);
    try {
      MountTable.Resolution resolution = mMountTable.resolve(path);
      AlluxioURI ufsUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufsClient = ufsResource.get();
        if (ufsClient.supportsActiveSync()) {
          SyncInfo syncInfo = ufsClient.getActiveSyncInfo(ufsUriList);
          // This returns a list of ufsUris that we need to sync.
          List<AlluxioURI> ufsSyncPoints = syncInfo.getSyncList();
          List<AlluxioURI> alluxioSyncPoints = ufsSyncPoints.stream()
              .map(mMountTable::reverseResolve).collect(Collectors.toList());
          for (AlluxioURI uri : alluxioSyncPoints) {
            LOG.debug("ready to sync {}", uri.getPath());
          }
        }
      }
    } catch (InvalidPathException e) {
      LOG.warn("Invalid path {}", path.getPath());
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
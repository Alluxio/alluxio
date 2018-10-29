package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.activesync.ActiveSyncManager;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Set;
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
    LOG.info("start Active Syncer heartbeat");

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

    LOG.info("filterList");
    for (AlluxioURI filter: filterList) {
      LOG.info("filterUri {}", filter.getPath());
    }
    for (AlluxioURI ufsUri: ufsUriList) {
      LOG.info("ufsUri {}", ufsUri.getPath());
    }

    AlluxioURI path = filterList.get(0);
    try {
      MountTable.Resolution resolution = mMountTable.resolve(path);
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufsClient = ufsResource.get();
        if (ufsClient.supportsActiveSync()) {
          SyncInfo syncInfo = ufsClient.getActiveSyncInfo(ufsUriList);
          // This returns a list of ufsUris that we need to sync.
          Set<AlluxioURI> ufsSyncPoints = syncInfo.getSyncList();

          for (AlluxioURI ufsUri : ufsSyncPoints) {
            LOG.info("sync {}", ufsUri.toString());
            AlluxioURI alluxioUri = mMountTable.reverseResolve(ufsUri);
            if (alluxioUri != null) {
              mFileSystemMaster.batchSyncMetadata(alluxioUri,
                  syncInfo.getFileUpdateList(ufsUri).stream().parallel()
                      .map(mMountTable::reverseResolve).collect(Collectors.toSet()));
            }

          }
        }
      }
    } catch (InvalidPathException e) {
      LOG.warn("Invalid path {}", path.getPath());
    } catch (Exception e) {
      LOG.warn("Exception " + Throwables.getStackTraceAsString(e));
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
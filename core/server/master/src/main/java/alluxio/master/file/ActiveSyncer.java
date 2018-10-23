package alluxio.master.file;

import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.activesync.ActiveSyncManager;
import alluxio.master.file.meta.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

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
    // The overview of this method is
    // 1. setup a source of event
    // 2. Filter based on the paths associated with this mountId
    // 3. Build History for each of the syncPoint
    // 4. If heurstics function returns sync, then we sync the syncPoint
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
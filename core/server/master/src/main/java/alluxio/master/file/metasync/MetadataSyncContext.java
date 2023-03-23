package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.RpcContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The context for the metadata sync.
 */
public class MetadataSyncContext {
  private final DescendantType mDescendantType;
  private final RpcContext mRpcContext;
  private boolean mAllowConcurrentModification = true;
  private FileSystemMasterCommonPOptions mCommonOptions;
  @Nullable
  private final String mStartAfter;
  private final Map<SyncOperation, Long> mFailedMap = new HashMap<>();
  private final Map<SyncOperation, Long> mSuccessMap = new HashMap<>();
  /** temporary one. */
  private final Set<AlluxioURI> mDirectoriesToUpdateIsLoaded = new ConcurrentHashSet<>();
  private Long mSyncStartTime = null;
  private Long mSyncFinishTime = null;
  private int mBatchSize = 1000;
  @Nullable
  private SyncFailReason mFailReason = null;

  /**
   * Creates a metadata sync context.
   * @param descendantType the sync descendant type (ALL/ONE/NONE)
   * @param rpcContext the rpc context
   * @param commonOptions the common options for TTL configurations
   * @param startAfter indicates where the sync starts, used on retries
   * @param batchSize the batch size to fetch the files from UFS
   */
  public MetadataSyncContext(
      DescendantType descendantType, RpcContext rpcContext,
      FileSystemMasterCommonPOptions commonOptions,
      @Nullable String startAfter, int batchSize) {
    mDescendantType = descendantType;
    mRpcContext = rpcContext;
    mCommonOptions = commonOptions;
    if (startAfter == null) {
      mStartAfter = null;
    } else {
      if (startAfter.startsWith(AlluxioURI.SEPARATOR)) {
        throw new RuntimeException("relative path is expected");
      }
      mStartAfter = startAfter;
      mBatchSize = batchSize;
      // TODO: need to format the startAfter like what we did for partial listing
      // Example:
      /*
      if (startAfter.startsWith(AlluxioURI.SEPARATOR)) {
        // this path starts from the root, so we must remove the prefix
        String startAfterCheck = startAfter.substring(0,
            Math.min(path.getPath().length(), startAfter.length()));
        if (!path.getPath().startsWith(startAfterCheck)) {
          throw new InvalidPathException(
              ExceptionMessage.START_AFTER_DOES_NOT_MATCH_PATH
                  .getMessage(startAfter, path.getPath()));
        }
        startAfter = startAfter.substring(
            Math.min(startAfter.length(), path.getPath().length()));
      }
       */
    }
  }

  /**
   * @return the descendant type of the sync
   * NONE -> only syncs the inode itself
   * ONE -> syncs the inode and its direct children
   * ALL -> recursively syncs a directory
   */
  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  /**
   * During the sync, the inodes might be updated by other requests concurrently, that makes
   * the sync operation stale. If the concurrent modification is allowed, these inodes will be
   * skipped, otherwise the sync will fail.
   * @return true, if the concurrent modification is allowed. Otherwise, false
   */
  public boolean isConcurrentModificationAllowed() {
    return mAllowConcurrentModification;
  }

  /**
   * @return the batch size to fetch the data from UFS
   */
  public int getBatchSize() {
    return mBatchSize;
  }

  /**
   * @return if the sync is a recursive sync
   */
  public boolean isRecursive() {
    return mDescendantType == DescendantType.ALL;
  }

  /**
   * @return the rpc context
   */
  public RpcContext getRpcContext() {
    return mRpcContext;
  }

  /**
   * @return the common options
   */
  public FileSystemMasterCommonPOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return null if the listing starts from the beginning, otherwise the start after path inclusive
   */
  @Nullable
  public String getStartAfter() {
    return mStartAfter;
  }

  /**
   * adds directories which are supposed to update is children loaded flag when the sync is done.
   * @param path the path
   */
  public void addDirectoriesToUpdateIsChildrenLoaded(AlluxioURI path) {
    mDirectoriesToUpdateIsLoaded.add(path);
  }

  /**
   * updates the direct children loaded flag for directories in a recursive sync.
   * @param inodeTree the inode tree
   */
  public void updateDirectChildrenLoaded(InodeTree inodeTree) throws InvalidPathException {
    for (AlluxioURI uri : mDirectoriesToUpdateIsLoaded) {
      try (LockedInodePath lockedInodePath =
               inodeTree.lockInodePath(
                   uri, InodeTree.LockPattern.WRITE_INODE, getRpcContext().getJournalContext())) {
        inodeTree.setDirectChildrenLoaded(
            () -> getRpcContext().getJournalContext(),
            lockedInodePath.getInode().asDirectory())
        ;
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * reports the completion of a successful sync operation
   * @param operation the operation
   */
  public void reportSyncOperationSuccess(SyncOperation operation) {
    mSuccessMap.put(operation, mSuccessMap.getOrDefault(operation, 0L) + 1);
  }

  /**
   * @param failReason the fail reason
   */
  public void setFailReason(SyncFailReason failReason) {
    mFailReason = failReason;
  }

  /**
   * Starts the metadata sync.
   */
  public void startSync() {
    mSyncStartTime = CommonUtils.getCurrentMs();
  }

  /**
   * Concludes the sync with a success.
   * @return the sync result
   */
  public SyncResult success() {
    Preconditions.checkNotNull(mSyncStartTime);
    mSyncFinishTime = CommonUtils.getCurrentMs();
    return new SyncResult(true, mSyncFinishTime - mSyncStartTime, mSuccessMap, mFailedMap, null);
  }

  /**
   * Concludes the sync with a fail.
   * @return the sync result
   */
  public SyncResult fail() {
    Preconditions.checkNotNull(mSyncStartTime);
    mSyncFinishTime = CommonUtils.getCurrentMs();
    return new SyncResult(false, mSyncFinishTime - mSyncStartTime, mSuccessMap, mFailedMap,
        mFailReason);
  }
}

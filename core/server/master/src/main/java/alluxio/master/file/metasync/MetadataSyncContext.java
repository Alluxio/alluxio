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
import javassist.runtime.Desc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class MetadataSyncContext {
  /*
  public enum DescendantType {
    ALL,
    ONE,
    NONE,
  }

   */

  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  public boolean isConcurrentModificationAllowed() {
    return mAllowConcurrentModification;
  }

  private boolean mAllowConcurrentModification;

  private final DescendantType mDescendantType;

  private final RpcContext mRpcContext;

  private FileSystemMasterCommonPOptions mCommonOptions;

  // TODO: need to format the startAfter like what we did for partial listing
  @Nullable
  private String mStartAfter;

  private Map<SyncOperation, Long> mFailedMap = new HashMap<>();
  private Map<SyncOperation, Long> mSuccessMap = new HashMap<>();
  private Set<AlluxioURI> mDirectoriesToUpdateIsLoaded = new ConcurrentHashSet<>();
  private Long mSyncStartTime = null;
  private Long mSyncFinishTime = null;

  @Nullable
  private SyncFailReason mFailReason = null;

  private AlluxioURI mSyncRootPath;

  public MetadataSyncContext(
      AlluxioURI syncRootPath,
      DescendantType descendantType, RpcContext rpcContext, FileSystemMasterCommonPOptions commonOptions,
      @Nullable String startAfter) {
    mSyncRootPath = syncRootPath;
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
      // TODO do something like this
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

  public boolean isRecursive() {
    return mDescendantType == DescendantType.ALL;
  }

  public RpcContext getRpcContext() {
    return mRpcContext;
  }

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


  // TODO do not update for now
  public void addDirectoriesToUpdateIsChildrenLoaded(AlluxioURI path) {
    mDirectoriesToUpdateIsLoaded.add(path);
  }

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

  public void reportSyncOperationSuccess(SyncOperation operation) {
    reportSyncOperationResult(operation, true);
  }

  public void setFailReason(SyncFailReason failReason) {
    mFailReason = failReason;
  }

  public void reportSyncOperationResult(SyncOperation operation, boolean success) {
    Map<SyncOperation, Long> map = success ? mSuccessMap : mFailedMap;
    map.put(operation, map.getOrDefault(operation, 0L) + 1);
  }

  public void startSync() {
    mSyncStartTime = CommonUtils.getCurrentMs();
  }

  public SyncResult success() {
    Preconditions.checkNotNull(mSyncStartTime);
    mSyncFinishTime = CommonUtils.getCurrentMs();
    return new SyncResult(true, mSyncFinishTime - mSyncStartTime, mSuccessMap, mFailedMap);
  }

  public SyncResult fail() {
    Preconditions.checkNotNull(mSyncStartTime);
    mSyncFinishTime = CommonUtils.getCurrentMs();
    return new SyncResult(false, mSyncFinishTime - mSyncStartTime, mSuccessMap, mFailedMap);
  }

  public AlluxioURI getSyncRootPath() {
    return mSyncRootPath;
  }
}

package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.ExceptionMessage;
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
  private final boolean mAllowConcurrentModification;
  private final FileSystemMasterCommonPOptions mCommonOptions;
  @Nullable
  private String mStartAfter;
  private final Map<SyncOperation, Long> mFailedMap = new HashMap<>();
  private final Map<SyncOperation, Long> mSuccessMap = new HashMap<>();
  private long mNumUfsFilesScanned = 0;
  /** temporary one. */
  private final Set<AlluxioURI> mDirectoriesToUpdateIsLoaded = new ConcurrentHashSet<>();
  private Long mSyncStartTime = null;
  private Long mSyncFinishTime = null;
  @Nullable
  private SyncFailReason mFailReason = null;

  /**
   * Creates a metadata sync context.
   * @param descendantType the sync descendant type (ALL/ONE/NONE)
   * @param rpcContext the rpc context
   * @param commonOptions the common options for TTL configurations
   * @param startAfter indicates where the sync starts (exclusive), used on retries
   */
  private MetadataSyncContext(
      DescendantType descendantType, RpcContext rpcContext,
      FileSystemMasterCommonPOptions commonOptions,
      @Nullable String startAfter,
      boolean allowConcurrentModification
  ) {
    mDescendantType = descendantType;
    mRpcContext = rpcContext;
    mCommonOptions = commonOptions;
    mAllowConcurrentModification = allowConcurrentModification;
    mStartAfter = startAfter;
  }

  public void validateStartAfter(AlluxioURI syncRoot) throws InvalidPathException {
    if (mStartAfter == null || !mStartAfter.startsWith(AlluxioURI.SEPARATOR)) {
      return;
    }
    // this path starts from the root, so we must remove the prefix
    String startAfterCheck = mStartAfter.substring(0,
        Math.min(syncRoot.getPath().length(), mStartAfter.length()));
    if (!syncRoot.getPath().startsWith(startAfterCheck)) {
      throw new InvalidPathException(
          ExceptionMessage.START_AFTER_DOES_NOT_MATCH_PATH
              .getMessage(mStartAfter, syncRoot.getPath()));
    }
    mStartAfter = mStartAfter.substring(
        Math.min(mStartAfter.length(), syncRoot.getPath().length()));
    if (mStartAfter.startsWith("/")) {
      mStartAfter = mStartAfter.substring(1);
    }
    if (mStartAfter.equals("")) {
      mStartAfter = null;
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
   * reports the completion of a successful sync operation.
   * @param operation the operation
   */
  public void reportSyncOperationSuccess(SyncOperation operation) {
    mSuccessMap.put(operation, mSuccessMap.getOrDefault(operation, 0L) + 1);
  }

  /**
   * reports a file from ufs has been scanned.
   */
  public void ufsFileScanned() {
    mNumUfsFilesScanned++;
    if (mNumUfsFilesScanned % 10000 == 0) {
      System.out.println(mNumUfsFilesScanned + " files scanned");
    }
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
    return new SyncResult(true, mSyncFinishTime - mSyncStartTime, mSuccessMap,
        mFailedMap, null, mNumUfsFilesScanned);
  }

  /**
   * Concludes the sync with a fail.
   * @return the sync result
   */
  public SyncResult fail() {
    Preconditions.checkNotNull(mSyncStartTime);
    mSyncFinishTime = CommonUtils.getCurrentMs();
    return new SyncResult(false, mSyncFinishTime - mSyncStartTime, mSuccessMap, mFailedMap,
        mFailReason, mNumUfsFilesScanned);
  }

  /**
   * Creates a builder.
   */
  public static class Builder {
    private DescendantType mDescendantType;
    private RpcContext mRpcContext;
    private FileSystemMasterCommonPOptions mCommonOptions = MetadataSyncer.NO_TTL_OPTION;
    private String mStartAfter = null;
    private boolean mAllowConcurrentModification = true;

    /**
     * Creates a builder.
     * @param rpcContext the rpc context
     * @param descendantType the descendant type
     * @return a new builder
     */
    public static Builder builder(RpcContext rpcContext, DescendantType descendantType) {
      Builder builder = new Builder();
      builder.mDescendantType = descendantType;
      builder.mRpcContext = rpcContext;
      return builder;
    }

    /**
     * @param descendantType the descendant type
     * @return the builder
     */
    public Builder setDescendantType(DescendantType descendantType) {
      mDescendantType = descendantType;
      return this;
    }

    /**
     * @param rpcContext the rpc context
     * @return builder
     */
    public Builder setRpcContext(RpcContext rpcContext) {
      mRpcContext = rpcContext;
      return this;
    }

    /**
     * @param commonOptions the common option
     * @return builder
     */
    public Builder setCommonOptions(FileSystemMasterCommonPOptions commonOptions) {
      mCommonOptions = commonOptions;
      return this;
    }

    /**
     * @param startAfter the start after
     * @return the builder
     */
    public Builder setStartAfter(String startAfter) {
      mStartAfter = startAfter;
      return this;
    }

    /**
     * @param allowModification the current modification is allowed
     * @return the builder
     */
    public Builder setAllowModification(boolean allowModification) {
      mAllowConcurrentModification = allowModification;
      return this;
    }

    /**
     * @return the built metadata sync context
     */
    public MetadataSyncContext build() {
      return new MetadataSyncContext(
          mDescendantType, mRpcContext, mCommonOptions,
          mStartAfter, mAllowConcurrentModification);
    }
  }
}

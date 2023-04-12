package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.BlockDeletionContext;
import alluxio.master.file.FileSystemJournalEntryMerger;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.OperationContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.journal.FileSystemMergeJournalContext;
import alluxio.master.journal.MetadataSyncMergeJournalContext;
import alluxio.master.mdsync.LoadResult;
import alluxio.master.mdsync.TaskInfo;
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
  static class MetadataSyncRpcContext extends RpcContext {
    public MetadataSyncRpcContext(
        BlockDeletionContext blockDeleter, MetadataSyncMergeJournalContext journalContext,
        OperationContext operationContext) {
      super(blockDeleter, journalContext, operationContext);
    }

    @Override
    public MetadataSyncMergeJournalContext getJournalContext() {
      return (MetadataSyncMergeJournalContext) super.getJournalContext();
    }
  }

  private final DescendantType mDescendantType;
  private final MetadataSyncRpcContext mRpcContext;
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
  private final TaskInfo mTaskInfo;
  private final LoadResult mLoadResult;

  /**
   * Creates a metadata sync context.
   * @param loadResult the load UFS result
   * @param rpcContext the rpc context
   * @param commonOptions the common options for TTL configurations
   * @param startAfter indicates where the sync starts (exclusive), used on retries
   */
  private MetadataSyncContext(
      LoadResult loadResult, MetadataSyncRpcContext rpcContext,
      FileSystemMasterCommonPOptions commonOptions,
      @Nullable String startAfter,
      boolean allowConcurrentModification
  ) {
    mDescendantType = loadResult.getTaskInfo().getDescendantType();
    mRpcContext = rpcContext;
    mCommonOptions = commonOptions;
    mAllowConcurrentModification = allowConcurrentModification;
    mStartAfter = startAfter;
    mTaskInfo = loadResult.getTaskInfo();
    mLoadResult = loadResult;
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
  public MetadataSyncRpcContext getRpcContext() {
    return mRpcContext;
  }

  /**
   * @return the metadata sync journal context
   */
  public MetadataSyncMergeJournalContext getMetadataSyncJournalContext() {
    return mRpcContext.getJournalContext();
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
    reportSyncOperationSuccess(operation, 1);
  }

  /**
   * reports the completion of a successful sync operation.
   * @param operation the operation
   * @param count the number of successes
   */
  public void reportSyncOperationSuccess(SyncOperation operation, long count) {
    mSuccessMap.put(operation, mSuccessMap.getOrDefault(operation, 0L) + count);
    mTaskInfo.getStats().reportSyncOperationSuccess(operation, count);
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
    long timestamp = CommonUtils.getCurrentMs();
    mSyncStartTime = timestamp;
    mTaskInfo.getStats().updateSyncStartTime(timestamp);
  }

  /**
   * Concludes the sync with a success.
   * @return the sync result
   */
  public SyncResult success() {
    Preconditions.checkNotNull(mSyncStartTime);
    long timestamp = CommonUtils.getCurrentMs();
    mSyncFinishTime = timestamp;
    mTaskInfo.getStats().updateSyncFinishTime(timestamp);
    return new SyncResult(true, mSyncStartTime, mSyncFinishTime, mSuccessMap,
        mFailedMap, null, mNumUfsFilesScanned);
  }

  public void reportSyncFailReason(SyncFailReason reason, Throwable t) {
    mTaskInfo.getStats().reportSyncFailReason(mLoadResult.getLoadRequest(), mLoadResult, reason, t);
  }

  /**
   * Creates a builder.
   */
  public static class Builder {
    private LoadResult mLoadResult;
    private MetadataSyncRpcContext mRpcContext;
    private FileSystemMasterCommonPOptions mCommonOptions = MetadataSyncer.NO_TTL_OPTION;
    private String mStartAfter = null;
    private boolean mAllowConcurrentModification = true;

    /**
     * Creates a builder.
     * @param rpcContext the rpc context
     * @param loadResult the load UFS result
     * @return a new builder
     */
    public static Builder builder(RpcContext rpcContext, LoadResult loadResult) {
      Preconditions.checkState(
          !(rpcContext.getJournalContext() instanceof FileSystemMergeJournalContext));
      Builder builder = new Builder();
      builder.mLoadResult = loadResult;
      /*
       * Wrap the journal context with a MetadataSyncMergeJournalContext, which behaves
       * differently in:
       *  1. the journals are merged and stayed in the context until it gets flushed
       *  2. when close() or flush() are called, the journal does not trigger a hard flush
       *  that commits the journals, instead, it only adds the journals to the async journal writer.
       *  During the metadata sync process, we are creating/updating many files, but we don't want
       *  to hard flush journals on every inode updates.
       */
      builder.mRpcContext = new MetadataSyncRpcContext(rpcContext.getBlockDeletionContext(),
          new MetadataSyncMergeJournalContext(rpcContext.getJournalContext(),
              new FileSystemJournalEntryMerger()), rpcContext.getOperationContext());
      return builder;
    }

    /**
     * @param rpcContext the rpc context
     * @return builder
     */
    public Builder setRpcContext(MetadataSyncRpcContext rpcContext) {
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
          mLoadResult, mRpcContext, mCommonOptions,
          mStartAfter, mAllowConcurrentModification);
    }
  }
}

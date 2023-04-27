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

package alluxio.master.file.mdsync;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.BlockDeletionContext;
import alluxio.master.file.FileSystemJournalEntryMerger;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.OperationContext;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.journal.FileSystemMergeJournalContext;
import alluxio.master.journal.MetadataSyncMergeJournalContext;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * The context for the metadata sync processing.
 */
public class SyncProcessContext implements Closeable {
  private final DescendantType mDescendantType;
  private final MetadataSyncRpcContext mRpcContext;
  private final RpcContext mBaseRpcContext;
  private final boolean mAllowConcurrentModification;
  private final FileSystemMasterCommonPOptions mCommonOptions;
  private final Set<AlluxioURI> mDirectoriesToUpdateAbsentCache = new ConcurrentHashSet<>();
  private final TaskInfo mTaskInfo;
  private final LoadResult mLoadResult;

  /**
   * Creates a metadata sync context.
   *
   * @param loadResult    the load UFS result
   * @param baseRpcContext the base rpc context
   * @param rpcContext    the metadata sync rpc context
   * @param commonOptions the common options for TTL configurations
   */
  private SyncProcessContext(
      LoadResult loadResult, RpcContext baseRpcContext, MetadataSyncRpcContext rpcContext,
      FileSystemMasterCommonPOptions commonOptions,
      boolean allowConcurrentModification
  ) {
    mDescendantType = loadResult.getLoadRequest().getDescendantType();
    mRpcContext = rpcContext;
    mBaseRpcContext = baseRpcContext;
    mCommonOptions = commonOptions;
    mAllowConcurrentModification = allowConcurrentModification;
    mTaskInfo = loadResult.getTaskInfo();
    mLoadResult = loadResult;
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
   *
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
   * adds directories which are supposed to update is children loaded flag when the sync is done.
   *
   * @param path the path
   */
  public void addDirectoriesToUpdateIsChildrenLoaded(AlluxioURI path) {
    mTaskInfo.addPathToUpdateDirectChildrenLoaded(path);
  }

  /**
   * adds directories which exists and needs to update the absent cache later.
   * @param path the path
   */
  public void addDirectoriesToUpdateAbsentCache(AlluxioURI path) {
    mDirectoriesToUpdateAbsentCache.add(path);
  }

  /**
   * Updates the absent cache and set directories existing.
   * @param ufsAbsentPathCache the absent cache
   */
  public void updateAbsentCache(UfsAbsentPathCache ufsAbsentPathCache) {
    for (AlluxioURI uri: mDirectoriesToUpdateAbsentCache) {
      ufsAbsentPathCache.processExisting(uri);
    }
  }

  /**
   * reports the completion of a successful sync operation.
   *
   * @param operation the operation
   */
  public void reportSyncOperationSuccess(SyncOperation operation) {
    reportSyncOperationSuccess(operation, 1);
  }

  /**
   * reports the completion of a successful sync operation.
   *
   * @param operation the operation
   * @param count     the number of successes
   */
  public void reportSyncOperationSuccess(SyncOperation operation, long count) {
    operation.getCounter().inc(count);
    mTaskInfo.getStats().reportSyncOperationSuccess(operation, count);
  }

  /**
   * Reports a fail reason leading to the sync failure.
   *
   * @param reason the reason
   * @param t the throwable
   */
  public void reportSyncFailReason(SyncFailReason reason, Throwable t) {
    mTaskInfo.getStats().reportSyncFailReason(mLoadResult.getLoadRequest(), mLoadResult, reason, t);
  }

  /**
   * @return the task info
   */
  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  @Override
  public void close() throws IOException {
    mRpcContext.close();
    mBaseRpcContext.close();
  }

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

  /**
   * Creates a builder.
   */
  public static class Builder {
    private LoadResult mLoadResult;
    private MetadataSyncRpcContext mRpcContext;
    private RpcContext mBaseRpcContext;
    private FileSystemMasterCommonPOptions mCommonOptions = DefaultSyncProcess.NO_TTL_OPTION;
    private boolean mAllowConcurrentModification = true;

    /**
     * Creates a builder.
     *
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
      builder.mBaseRpcContext = rpcContext;
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
    public SyncProcessContext build() {
      return new SyncProcessContext(
          mLoadResult, mBaseRpcContext, mRpcContext, mCommonOptions,
          mAllowConcurrentModification);
    }
  }
}

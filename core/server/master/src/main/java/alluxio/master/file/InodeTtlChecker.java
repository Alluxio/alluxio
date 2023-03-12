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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.TtlAction;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.ProtobufUtils;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournalContext;
import alluxio.proto.journal.File.UpdateInodeEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents the executor for periodic inode ttl check.
 */
@NotThreadSafe
final class InodeTtlChecker implements HeartbeatExecutor {
  public static final int JOURNAL_FLUSH_BATCH_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(InodeTtlChecker.class);

  private final FileSystemMaster mFileSystemMaster;
  private final InodeTree mInodeTree;
  private final TtlBucketList mTtlBuckets;

  /**
   * Constructs a new {@link InodeTtlChecker}.
   */
  public InodeTtlChecker(FileSystemMaster fileSystemMaster, InodeTree inodeTree) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mTtlBuckets = inodeTree.getTtlBuckets();
  }

  @Override
  public void heartbeat() throws InterruptedException {
    Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
    // It is hard to measure the exact journal entries on processing each inode
    // So this counts the processed inodes instead of exact journal entries
    int processedFiles = 0;
    boolean success = false;
    try (JournalContext journalContext = mFileSystemMaster.createJournalContext()) {
      for (TtlBucket bucket : expiredBuckets) {
        for (Inode inode : bucket.getInodes()) {
          // Throw if interrupted.
          if (Thread.interrupted()) {
            throw new InterruptedException("InodeTtlChecker interrupted.");
          }

          // Manually flush to avoid stacking up too many journal entries
          if (processedFiles > JOURNAL_FLUSH_BATCH_SIZE) {
            journalContext.flush();
            processedFiles = 0;
          }

          AlluxioURI path = null;
          try (LockedInodePath inodePath =
               mInodeTree.lockFullInodePath(
                   inode.getId(), LockPattern.READ, NoopJournalContext.INSTANCE)
          ) {
            path = inodePath.getUri();
          } catch (FileDoesNotExistException e) {
            // The inode has already been deleted, nothing needs to be done.
            continue;
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                e.toString());
          }

          if (path != null) {
            processedFiles += processPath(journalContext, inode, path);
          }
        }
      }
      success = true;
    } catch (UnavailableException e) {
      // Journal is off due to failover or master shutdown
      LOG.warn("Journal is not available. Cannot perform TTL checks.");
    }

    if (success) {
      // By reaching here, the journal entries are flushed
      mTtlBuckets.removeBuckets(expiredBuckets);
    }
  }

  /**
   * Will acquire WRITE_INODE lock internally when performing the delete/free action.
   */
  private long processPath(JournalContext journalContext, Inode inode, AlluxioURI path) {
    try {
      TtlAction ttlAction = inode.getTtlAction();
      LOG.info("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
      switch (ttlAction) {
        case FREE: // Default: FREE
          return handleFree(inode, path, journalContext);
        case DELETE:
          return handleDelete(inode, path, journalContext);
        case DELETE_ALLUXIO:
          return handleDeleteCache(inode, path, journalContext);
        default:
          LOG.error("Unknown ttl action {}", ttlAction);
      }
    } catch (Exception e) {
      LOG.error("Exception trying to clean up {} for ttl check", inode, e);
    }
    return 0L;
  }

  private long handleFree(Inode inode, AlluxioURI path, JournalContext journalContext)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, IOException {
    long processedFiles;
    if (inode.isDirectory()) {
      mFileSystemMaster.free(path, FreeContext
          .mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)),
              journalContext);
      processedFiles = ((InodeDirectory) inode).getChildCount() + 1;
    } else {
      mFileSystemMaster.free(path,
          FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true)),
              journalContext);
      processedFiles = 1;
    }
    // Reset state
    mInodeTree.updateInode(journalContext, UpdateInodeEntry.newBuilder()
        .setId(inode.getId())
        .setTtl(Constants.NO_TTL)
        .setTtlAction(ProtobufUtils.toProtobuf(TtlAction.DELETE))
        .build());
    mTtlBuckets.remove(inode);
    return processedFiles;
  }

  private long handleDelete(Inode inode, AlluxioURI path, JournalContext journalContext)
      throws FileDoesNotExistException, AccessControlException, DirectoryNotEmptyException,
      IOException, InvalidPathException {
    if (inode.isDirectory()) {
      mFileSystemMaster.delete(path,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)),
          journalContext);
      return ((InodeDirectory) inode).getChildCount() + 1;
    } else {
      mFileSystemMaster.delete(path, DeleteContext.defaults(), journalContext);
      return 1L;
    }
  }

  private long handleDeleteCache(Inode inode, AlluxioURI path, JournalContext journalContext)
      throws FileDoesNotExistException, AccessControlException, DirectoryNotEmptyException,
      IOException, InvalidPathException  {
    if (inode.isDirectory()) {
      mFileSystemMaster.delete(path,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder()
              .setRecursive(true).setAlluxioOnly(true)), journalContext);
      return ((InodeDirectory) inode).getChildCount() + 1;
    } else {
      mFileSystemMaster.delete(path,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder()
              .setAlluxioOnly(true)), journalContext);
      return 1L;
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}

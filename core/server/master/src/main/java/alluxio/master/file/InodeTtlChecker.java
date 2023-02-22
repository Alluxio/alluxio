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
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.TtlAction;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.ProtobufUtils;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.meta.Inode;
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

import java.util.HashSet;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents the executor for periodic inode ttl check.
 */
@NotThreadSafe
final class InodeTtlChecker implements HeartbeatExecutor {
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
    Set<TtlBucket> expiredBuckets = mTtlBuckets.pollExpiredBuckets(System.currentTimeMillis());
    Set<Inode> failedInodes = new HashSet();
    for (TtlBucket bucket : expiredBuckets) {
        for (long inodeId : bucket.getInodes()) {
        // Throw if interrupted.
        if (Thread.interrupted()) {
          throw new InterruptedException("InodeTtlChecker interrupted.");
        }
        AlluxioURI path = null;
        try (LockedInodePath inodePath =
            mInodeTree.lockFullInodePath(
                inodeId, LockPattern.READ, NoopJournalContext.INSTANCE)
        ) {
          path = inodePath.getUri();
        } catch (FileDoesNotExistException e) {
          // The inode has already been deleted, nothing needs to be done.
          continue;
        } catch (Exception e) {
          LOG.error("Exception trying to clean up inode:{},path:{} for ttl check: {}", inodeId,
              path, e.toString());
        }
        if (path != null) {
          Inode inode = null;
          try {
            inode = mTtlBuckets.loadInode(inodeId);
            // Check again if this inode is indeed expired.
            if (inode == null || inode.getTtl() == Constants.NO_TTL
                || inode.getCreationTimeMs() + inode.getTtl() > System.currentTimeMillis())
              continue;
            TtlAction ttlAction = inode.getTtlAction();
            LOG.info("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
            switch (ttlAction) {
              case FREE:
                // public free method will lock the path, and check WRITE permission required at
                // parent of file
                if (inode.isDirectory()) {
                  mFileSystemMaster.free(path, FreeContext
                      .mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
                } else {
                  mFileSystemMaster.free(path,
                      FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true)));
                }
                try (JournalContext journalContext = mFileSystemMaster.createJournalContext()) {
                  // Reset state
                  mInodeTree.updateInode(journalContext, UpdateInodeEntry.newBuilder()
                      .setId(inode.getId())
                      .setTtl(Constants.NO_TTL)
                      .setTtlAction(ProtobufUtils.toProtobuf(TtlAction.DELETE))
                      .build());
                }
                break;
              case DELETE:
                // public delete method will lock the path, and check WRITE permission required at
                // parent of file
                if (inode.isDirectory()) {
                  mFileSystemMaster.delete(path,
                      DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
                } else {
                  mFileSystemMaster.delete(path, DeleteContext.defaults());
                }
                break;
              case DELETE_ALLUXIO: // Default: DELETE_ALLUXIO
                // public delete method will lock the path, and check WRITE permission required at
                // parent of file
                if (inode.isDirectory()) {
                  mFileSystemMaster.delete(path,
                          DeleteContext.mergeFrom(DeletePOptions.newBuilder()
                                  .setRecursive(true).setAlluxioOnly(true)));
                } else {
                  mFileSystemMaster.delete(path,
                          DeleteContext.mergeFrom(DeletePOptions.newBuilder()
                                  .setAlluxioOnly(true)));
                }
                break;
              default:
                LOG.error("Unknown ttl action {}", ttlAction);
            }
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check", path, e);
            if (inode != null) {
              failedInodes.add(inode);
            }
          }
        }
      }
    }
    // Put back those failed-to-expire inodes for next round retry.
    if (!failedInodes.isEmpty()) {
      for(Inode inode : failedInodes) {
        mTtlBuckets.insert(inode);
      }
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}

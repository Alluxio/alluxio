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
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.wire.TtlAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public InodeTtlChecker(FileSystemMaster fileSystemMaster, InodeTree inodeTree,
      TtlBucketList ttlBuckets) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mTtlBuckets = ttlBuckets;
  }

  @Override
  public void heartbeat() {
    Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
    for (TtlBucket bucket : expiredBuckets) {
      for (Inode inode : bucket.getInodes()) {
        AlluxioURI path = null;
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(inode.getId(), InodeTree.LockMode.READ)) {
          path = inodePath.getUri();
        } catch (Exception e) {
          LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
              e.toString());
        }
        if (path != null) {
          try {
            TtlAction ttlAction = inode.getTtlAction();
            LOG.debug("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
            switch (ttlAction) {
              case FREE:
                // public free method will lock the path, and check WRITE permission required at
                // parent of file
                if (inode.isDirectory()) {
                  mFileSystemMaster
                      .free(path, FreeOptions.defaults().setForced(true).setRecursive(true));
                } else {
                  mFileSystemMaster.free(path, FreeOptions.defaults().setForced(true));
                }
                // Reset state
                inode.setTtl(Constants.NO_TTL);
                inode.setTtlAction(TtlAction.DELETE);
                mTtlBuckets.remove(inode);
                break;
              case DELETE:// Default if not set is DELETE
                // public delete method will lock the path, and check WRITE permission required at
                // parent of file
                if (inode.isDirectory()) {
                  mFileSystemMaster.delete(path, DeleteOptions.defaults().setRecursive(true));
                } else {
                  mFileSystemMaster.delete(path, DeleteOptions.defaults().setRecursive(false));
                }
                break;
              default:
                LOG.error("Unknown ttl action {}", ttlAction);
            }
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check", inode.toString(), e);
          }
        }
      }
    }
    mTtlBuckets.removeBuckets(expiredBuckets);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}

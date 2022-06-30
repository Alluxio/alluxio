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

import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.JournalContext;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.util.IdUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Lost files periodic check.
 */
@NotThreadSafe
final class LostFileDetector implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(LostFileDetector.class);
  private final FileSystemMaster mFileSystemMaster;
  private final InodeTree mInodeTree;
  private final BlockMaster mBlockMaster;

  /**
   * Constructs a new {@link LostFileDetector}.
   */
  public LostFileDetector(FileSystemMaster fileSystemMaster, BlockMaster blockMaster,
      InodeTree inodeTree) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mBlockMaster = blockMaster;
    MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_LOST_FILE_COUNT.getName(),
        () -> mFileSystemMaster.getLostFiles().size());
  }

  @Override
  public void heartbeat() throws InterruptedException {
    Iterator<Long> iter = mBlockMaster.getLostBlocksIterator();
    Set<Long> lostFiles = new HashSet<>();
    while (iter.hasNext()) {
      long blockId = iter.next();
      long containerId = BlockId.getContainerId(blockId);
      long fileId = IdUtils.createFileId(containerId);
      lostFiles.add(fileId);
      iter.remove();
    }
    for (long fileId : lostFiles) {
      // Throw if interrupted.
      if (Thread.interrupted()) {
        throw new InterruptedException("LostFileDetector interrupted.");
      }
      boolean markAsLost = false;
      try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, LockPattern.READ)) {
        Inode inode = inodePath.getInode();
        if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
          markAsLost = true;
        }
        iter.remove();
      } catch (FileDoesNotExistException e) {
        LOG.debug("Exception trying to get inode from inode tree", e);
      }

      if (markAsLost) {
        // update the state
        try (JournalContext journalContext = mFileSystemMaster.createJournalContext();
             LockedInodePath inodePath =
                 mInodeTree.lockFullInodePath(fileId, LockPattern.WRITE_INODE)) {
          Inode inode = inodePath.getInode();
          if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
            mInodeTree.updateInode(journalContext,
                UpdateInodeEntry.newBuilder().setId(inode.getId())
                    .setPersistenceState(PersistenceState.LOST.name()).build());
            lostFiles.add(fileId);
          }
        } catch (FileDoesNotExistException e) {
          LOG.debug("Exception trying to get inode from inode tree", e);
        } catch (UnavailableException e) {
          LOG.warn("Journal is not available. Backing off. Error: {}", e.toString());
          break;
        }
      }
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}

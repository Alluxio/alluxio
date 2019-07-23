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

import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents the executor for periodic inode ttl check.
 */
@NotThreadSafe
final class AccessTimeUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(AccessTimeUpdater.class);

  private final long mFlushInterval;
  private final long mUpdatePrecision;
  private final long mShutdownTimeout;

  private final FileSystemMaster mFileSystemMaster;
  private final InodeTree mInodeTree;

  /** Keep track of all inodes that need access time update. */
  private ConcurrentHashSet<Long> mAccessTimeUpdates;
  private ScheduledExecutorService mExecutorService = null;
  private AtomicBoolean mUpdateScheduled = new AtomicBoolean();

  /**
   * Constructs a new {@link AccessTimeUpdater}.
   */
  public AccessTimeUpdater(FileSystemMaster fileSystemMaster, InodeTree inodeTree) {
    this(fileSystemMaster, inodeTree,
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL),
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION),
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT));
  }

  /**
   * Constructs a new {@link AccessTimeUpdater} with time configurations.
   */
  @VisibleForTesting
  public AccessTimeUpdater(FileSystemMaster fileSystemMaster, InodeTree inodeTree,
      long flushInterval, long updatePrecision, long shutdownTimeout) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mAccessTimeUpdates = new ConcurrentHashSet<>();
    mFlushInterval = flushInterval;
    mUpdatePrecision = updatePrecision;
    mShutdownTimeout = shutdownTimeout;
  }

  public void start() {
    start(mFlushInterval > 0 ? Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("AccessTimeUpdater-%d", true)) : null);
  }

  @VisibleForTesting
  public void start(ScheduledExecutorService executorService) {
    mExecutorService = executorService;
  }

  public void stop() {
    if (mExecutorService != null) {
      flushUpdates();
      ThreadUtils.shutdownAndAwaitTermination(mExecutorService, mShutdownTimeout);
    }
  }

  /**
   * Update inode last access time.
   *
   * @param context the journal context
   * @param inode the target inode
   * @param opTimeMs the last access time to set on the inode
   */
  public void updateAccessTime(JournalContext context, Inode inode, long opTimeMs) {
    if (opTimeMs - inode.getLastAccessTimeMs() > mUpdatePrecision) {
      try (LockResource lr = mInodeTree.getInodeLockManager().lockUpdate(inode.getId())) {
        UpdateInodeEntry entry = mInodeTree.updateInodeAccessTime(inode.getId(), opTimeMs);
        if (mExecutorService != null) {
          scheduleJournalUpdate(entry);
        } else {
          journalAccessTime(context, entry);
        }
      }
    }
  }

  private void scheduleJournalUpdate(UpdateInodeEntry entry) {
    mAccessTimeUpdates.add(entry.getId());
    if (mUpdateScheduled.compareAndSet(false, true)) {
      mExecutorService.schedule(this::flushUpdates, mFlushInterval, TimeUnit.MILLISECONDS);
    }
  }

  private void journalAccessTime(JournalContext context, UpdateInodeEntry entry) {
    context.append(Journal.JournalEntry.newBuilder().setUpdateInode(entry)
        .build());
  }

  private void flushUpdates() {
    long filesRemoved = 0;
    mUpdateScheduled.set(false);
    try (JournalContext context = mFileSystemMaster.createJournalContext()) {
      for (Iterator<Long> iterator = mAccessTimeUpdates.iterator();
           iterator.hasNext();) {
        long inodeId = iterator.next();
        iterator.remove();
        try (LockedInodePath path = mInodeTree.lockFullInodePath(inodeId, LockPattern.READ);
             LockResource lr = mInodeTree.getInodeLockManager().lockUpdate(inodeId)) {
          journalAccessTime(context, UpdateInodeEntry.newBuilder()
              .setId(inodeId)
              .setLastAccessTimeMs(path.getInode().getLastAccessTimeMs())
              .build());
        } catch (FileDoesNotExistException e) {
          // Some inodes cannot be updated because they are removed from the file system.
          filesRemoved++;
        }
      }
    } catch (UnavailableException e) {
      LOG.debug("Failed to flush access time updates.", e);
    }
    if (filesRemoved > 0) {
      LOG.debug(
          "Access time for {} files cannot be updated because they are removed from file system",
          filesRemoved);
    }
  }
}

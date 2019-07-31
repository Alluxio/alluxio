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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.sink.JournalSink;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.Journal;
import alluxio.resource.LockResource;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles the update of inode last access time.
 */
@NotThreadSafe
final class AccessTimeUpdater implements JournalSink {
  private static final Logger LOG = LoggerFactory.getLogger(AccessTimeUpdater.class);

  private final long mFlushInterval;
  private final long mUpdatePrecision;
  private final long mShutdownTimeout;

  private final FileSystemMaster mFileSystemMaster;
  private final InodeTree mInodeTree;

  /** Keep track of all inodes that need access time update. */
  private ConcurrentHashMap<Long, Long> mAccessTimeUpdates;
  private ScheduledExecutorService mExecutorService = null;
  private AtomicBoolean mUpdateScheduled = new AtomicBoolean();

  /**
   * Constructs a new {@link AccessTimeUpdater}.
   */
  public AccessTimeUpdater(FileSystemMaster fileSystemMaster, InodeTree inodeTree,
      JournalSystem journalSystem) {
    this(fileSystemMaster, inodeTree, journalSystem,
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL),
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION),
        ServerConfiguration.getMs(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATER_SHUTDOWN_TIMEOUT));
  }

  /**
   * Constructs a new {@link AccessTimeUpdater} with time configurations.
   */
  @VisibleForTesting
  public AccessTimeUpdater(FileSystemMaster fileSystemMaster, InodeTree inodeTree,
      JournalSystem journalSystem, long flushInterval, long updatePrecision, long shutdownTimeout) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mAccessTimeUpdates = new ConcurrentHashMap<>();
    mFlushInterval = flushInterval;
    mUpdatePrecision = updatePrecision;
    mShutdownTimeout = shutdownTimeout;
    journalSystem.addJournalSink(mFileSystemMaster, this);
  }

  public void start() {
    start(mFlushInterval > 0 ? Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("AccessTimeUpdater-%d", true)) : null);
  }

  @VisibleForTesting
  public synchronized void start(ScheduledExecutorService executorService) {
    if (mExecutorService != null && mExecutorService != executorService
        && !mExecutorService.isShutdown()) {
      stop();
    }
    mExecutorService = executorService;
  }

  @Override
  public void beforeShutdown() {
    if (mExecutorService != null) {
      flushUpdates();
    }
  }

  public synchronized void stop() {
    if (mExecutorService != null) {
      ThreadUtils.shutdownAndAwaitTermination(mExecutorService, mShutdownTimeout);
    }
  }

  /**
   * Update inode last access time. Requires at least read lock acquired for the inode.
   *
   * @param context the journal context
   * @param inode the target inode
   * @param opTimeMs the last access time to set on the inode
   */
  public void updateAccessTime(JournalContext context, Inode inode, long opTimeMs) {
    if (opTimeMs - inode.getLastAccessTimeMs() > mUpdatePrecision) {
      try (LockResource lr = mInodeTree.getInodeLockManager().lockUpdate(inode.getId())) {
        if (mExecutorService != null) {
          // journal update asynchronously
          UpdateInodeEntry entry = mInodeTree.updateInodeAccessTimeNoJournal(inode.getId(),
              opTimeMs);
          scheduleJournalUpdate(entry);
        } else {
          mInodeTree.updateInode(context, UpdateInodeEntry.newBuilder()
                  .setId(inode.getId())
                  .setLastAccessTimeMs(opTimeMs)
                  .build());
        }
      }
    }
  }

  private void scheduleJournalUpdate(UpdateInodeEntry entry) {
    mAccessTimeUpdates.put(entry.getId(), entry.getLastAccessTimeMs());
    if (mUpdateScheduled.compareAndSet(false, true)) {
      mExecutorService.schedule(this::flushScheduledUpdates, mFlushInterval, TimeUnit.MILLISECONDS);
    }
  }

  private void flushScheduledUpdates() {
    mUpdateScheduled.set(false);
    flushUpdates();
  }

  private void flushUpdates() {
    try (JournalContext context = mFileSystemMaster.createJournalContext()) {
      for (Iterator<Map.Entry<Long, Long>> iterator = mAccessTimeUpdates.entrySet().iterator();
           iterator.hasNext();) {
        Map.Entry<Long, Long> inodeEntry = iterator.next();
        iterator.remove();
        UpdateInodeEntry entry = UpdateInodeEntry.newBuilder()
            .setId(inodeEntry.getKey())
            .setLastAccessTimeMs(inodeEntry.getValue())
            .build();
        context.append(Journal.JournalEntry.newBuilder().setUpdateInode(entry)
            .build());
      }
    } catch (UnavailableException e) {
      LOG.debug("Failed to flush access time updates.", e);
    }
  }
}

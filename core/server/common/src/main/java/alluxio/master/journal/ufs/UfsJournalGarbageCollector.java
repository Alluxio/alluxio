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

package alluxio.master.journal.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A garbage collector that periodically snapshots the journal and deletes files that are not
 * necessary anymore. The implementation guarantees that the journal contains all the information
 * required to recover the master full state.
 */
@ThreadSafe
final class UfsJournalGarbageCollector implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalGarbageCollector.class);

  private final ScheduledExecutorService mExecutor = Executors.newSingleThreadScheduledExecutor(
      ThreadFactoryUtils.build("UfsJournalGarbageCollector-%d", true));
  private final UfsJournal mJournal;
  private final UnderFileSystem mUfs;
  private ScheduledFuture<?> mGc;

  /**
   * Creates the {@link UfsJournalGarbageCollector} instance.
   *
   * @param journal the UFS journal handle
   */
  UfsJournalGarbageCollector(UfsJournal journal) {
    mJournal = Preconditions.checkNotNull(journal, "journal");
    mUfs = mJournal.getUfs();
    mGc = mExecutor.scheduleAtFixedRate(this::gc,
        Constants.SECOND_MS, ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_GC_PERIOD_MS),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (mGc != null) {
      mGc.cancel(true);
      mGc = null;
    }
    mExecutor.shutdown();
  }

  /**
   * Deletes unneeded snapshots.
   */
  void gc() {
    UfsJournalSnapshot snapshot;
    try {
      snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    } catch (IOException e) {
      LOG.warn("Failed to get journal snapshot with error {}.", e.toString());
      return;
    }
    long checkpointSequenceNumber = 0;

    // Checkpoint.
    List<UfsJournalFile> checkpoints = snapshot.getCheckpoints();
    if (!checkpoints.isEmpty()) {
      checkpointSequenceNumber = checkpoints.get(checkpoints.size() - 1).getEnd();
    }
    for (int i = 0; i < checkpoints.size() - 1; i++) {
      // Only keep at most 2 checkpoints.
      if (i < checkpoints.size() - 2) {
        deleteNoException(checkpoints.get(i).getLocation());
      }
      // For the the second last checkpoint. Check whether it has been there for a long time.
      gcFileIfStale(checkpoints.get(i), checkpointSequenceNumber);
    }

    for (UfsJournalFile log : snapshot.getLogs()) {
      gcFileIfStale(log, checkpointSequenceNumber);
    }

    for (UfsJournalFile tmpCheckpoint : snapshot.getTemporaryCheckpoints()) {
      gcFileIfStale(tmpCheckpoint, checkpointSequenceNumber);
    }
  }

  /**
   * Garbage collects a file if necessary.
   *
   * @param file the file
   * @param checkpointSequenceNumber the first sequence number that has not been checkpointed
   */
  private void gcFileIfStale(UfsJournalFile file, long checkpointSequenceNumber) {
    if (file.getEnd() > checkpointSequenceNumber && !file.isTmpCheckpoint()) {
      return;
    }

    long lastModifiedTimeMs;
    try {
      lastModifiedTimeMs = mUfs.getFileStatus(file.getLocation().toString()).getLastModifiedTime();
    } catch (IOException e) {
      LOG.warn("Failed to get the last modified time for {}.", file.getLocation());
      return;
    }

    long thresholdMs = file.isTmpCheckpoint()
        ? ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS)
        : ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_GC_THRESHOLD_MS);

    if (System.currentTimeMillis() - lastModifiedTimeMs > thresholdMs) {
      deleteNoException(file.getLocation());
    }
  }

  /**
   * Deletes a file and swallows the exception by logging it.
   *
   * @param location the file location
   */
  private void deleteNoException(URI location) {
    try {
      mUfs.deleteFile(location.toString());
      LOG.info("Garbage collected journal file {}.", location);
    } catch (IOException e) {
      LOG.warn("Failed to garbage collect journal file {}.", location);
    }
  }
}

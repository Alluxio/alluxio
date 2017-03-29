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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.journal.JournalWriter;
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
 * Implementation of {@link JournalWriter} based on UFS.
 */
@ThreadSafe
final class UfsJournalGarbageCollector implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalGarbageCollector.class);

  private ScheduledExecutorService mExecutor = Executors.newSingleThreadScheduledExecutor(
      ThreadFactoryUtils.build("UfsJournalGarbageCollector-%d", true));

  private UfsJournal mJournal;

  private ScheduledFuture<?> mGc;

  UfsJournalGarbageCollector(UfsJournal journal) {
    mJournal = journal;

    mGc = mExecutor.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            gc();
          }
        }, Constants.SECOND_MS, Configuration.getLong(PropertyKey.MASTER_JOURNAL_GC_PERIOD_MS),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (mGc != null) {
      mGc.cancel(true);
      mGc = null;
    }
  }

  private void gc() {
    UfsJournal.Snapshot snapshot;
    try {
      snapshot = mJournal.getSnapshot();
    } catch (IOException e) {
      LOG.warn("Failed to get journal snapshot with error {}.", e.getMessage());
      return;
    }
    long nextCheckpointSequenceNumber = 0;

    // Checkpoint.
    List<UfsJournalFile> checkpoints = snapshot.mCheckpoints;
    if (!checkpoints.isEmpty()) {
      nextCheckpointSequenceNumber = checkpoints.get(checkpoints.size() - 1).getEnd();
    }
    for (int i = 0; i < checkpoints.size() - 1; ++i) {
      // Only keep at most 2 checkpoints.
      if (i != checkpoints.size() - 2) {
        deleteNoException(checkpoints.get(i).getLocation());
      }
      // For the the second last checkpoint. Check whether it has been there for a long time.
      maybeGc(checkpoints.get(i), nextCheckpointSequenceNumber);
    }

    for (UfsJournalFile log : snapshot.mLogs) {
      maybeGc(log, nextCheckpointSequenceNumber);
    }

    for (UfsJournalFile tmpCheckpoint : snapshot.mTemporaryCheckpoints) {
      maybeGc(tmpCheckpoint, nextCheckpointSequenceNumber);
    }
  }

  private void maybeGc(UfsJournalFile file, long nextCheckpointSequenceNumber) {
    if (file.getEnd() > nextCheckpointSequenceNumber && !file.isTmpCheckpoint()) {
      return;
    }

    long lastModifiedTimeMs;
    try {
      lastModifiedTimeMs = mJournal.getUfs().getModificationTimeMs(file.getLocation().toString());
    } catch (IOException e) {
      LOG.warn("Failed to get the last modified time for {}.", file.getLocation());
      return;
    }

    long thresholdMs = file.isTmpCheckpoint() ?
        Configuration.getLong(PropertyKey.MASTER_JOURNAL_TEMPORARY_FILE_GC_THRESHOLD_MS) :
        Configuration.getLong(PropertyKey.MASTER_JOURNAL_GC_THRESHOLD_MS);

    if (System.currentTimeMillis() - lastModifiedTimeMs > thresholdMs) {
      deleteNoException(file.getLocation());
    }
  }

  void deleteNoException(URI location) {
    try {
      mJournal.getUfs().deleteFile(location.toString());
    } catch (IOException e) {
      LOG.warn("Failed to delete journal file {}.", location);
    }
  }
}

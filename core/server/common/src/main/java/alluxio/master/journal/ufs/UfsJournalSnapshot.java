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

import alluxio.underfs.UfsStatus;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a snapshot of everything in the journal and some utility methods.
 */
@ThreadSafe
@VisibleForTesting
public final class UfsJournalSnapshot {
  /** The committed checkpoints. */
  private final List<UfsJournalFile> mCheckpoints;
  /** The journal edit logs including the incomplete log. */
  private final List<UfsJournalFile> mLogs;
  /** The temporary checkpoint files. */
  private final List<UfsJournalFile> mTemporaryCheckpoints;

  /**
   * Creates an instance of the journal snapshot.
   *
   * @param checkpoints the checkpoints
   * @param logs the logs including the incomplete log
   * @param temporaryCheckpoints the temporary checkpoint files
   */
  private UfsJournalSnapshot(List<UfsJournalFile> checkpoints, List<UfsJournalFile> logs,
      List<UfsJournalFile> temporaryCheckpoints) {
    mCheckpoints = checkpoints;
    mLogs = logs;
    mTemporaryCheckpoints = temporaryCheckpoints;
  }

  /**
   * @return the checkpoints sorted by end sequence number increasingly
   */
  public List<UfsJournalFile> getCheckpoints() {
    return mCheckpoints;
  }

  /**
   * @return the latest checkpoint, null if no checkpoint exists
   */
  public UfsJournalFile getLatestCheckpoint() {
    if (!mCheckpoints.isEmpty()) {
      return mCheckpoints.get(mCheckpoints.size() - 1);
    }
    return null;
  }

  /**
   * @return the logs sorted by the end sequence number increasingly
   */
  public List<UfsJournalFile> getLogs() {
    return mLogs;
  }

  /**
   * @return the temporary checkpoints
   */
  public List<UfsJournalFile> getTemporaryCheckpoints() {
    return mTemporaryCheckpoints;
  }

  /**
   * Creates a snapshot of the journal.
   *
   * @param journal the journal
   * @return the journal snapshot
   */
  public static UfsJournalSnapshot getSnapshot(UfsJournal journal) throws IOException {
    // Checkpoints.
    List<UfsJournalFile> checkpoints = new ArrayList<>();
    UfsStatus[] statuses = journal.getUfs().listStatus(journal.getCheckpointDir().toString());
    if (statuses != null) {
      for (UfsStatus status : statuses) {
        UfsJournalFile file = UfsJournalFile.decodeCheckpointFile(journal, status.getName());
        if (file != null) {
          checkpoints.add(file);
        }
      }
      Collections.sort(checkpoints);
    }

    List<UfsJournalFile> logs = new ArrayList<>();
    statuses = journal.getUfs().listStatus(journal.getLogDir().toString());
    if (statuses != null) {
      for (UfsStatus status : statuses) {
        UfsJournalFile file = UfsJournalFile.decodeLogFile(journal, status.getName());
        if (file != null) {
          logs.add(file);
        }
      }
      Collections.sort(logs);
    }

    List<UfsJournalFile> tmpCheckpoints = new ArrayList<>();
    statuses = journal.getUfs().listStatus(journal.getTmpDir().toString());
    if (statuses != null) {
      for (UfsStatus status : statuses) {
        tmpCheckpoints.add(UfsJournalFile.decodeTemporaryCheckpointFile(journal, status.getName()));
      }
    }

    return new UfsJournalSnapshot(checkpoints, logs, tmpCheckpoints);
  }

  /**
   * Gets the current log (the incomplete log) that is being written to.
   *
   * @param journal the journal
   * @return the current log
   */
  @VisibleForTesting
  public static UfsJournalFile getCurrentLog(UfsJournal journal) throws IOException {
    List<UfsJournalFile> logs = new ArrayList<>();
    UfsStatus[] statuses = journal.getUfs().listStatus(journal.getLogDir().toString());
    if (statuses != null) {
      for (UfsStatus status : statuses) {
        UfsJournalFile file = UfsJournalFile.decodeLogFile(journal, status.getName());
        if (file != null) {
          logs.add(file);
        }
      }
      if (!logs.isEmpty()) {
        UfsJournalFile file = Collections.max(logs);
        if (file.isIncompleteLog()) {
          return file;
        }
      }
    }
    return null;
  }

  /**
   * Gets the first journal log sequence number that is not yet checkpointed.
   *
   * @return the first journal log sequence number that is not yet checkpointed
   */
  static long getNextLogSequenceNumberToCheckpoint(UfsJournal journal) throws IOException {
    List<UfsJournalFile> checkpoints = new ArrayList<>();
    UfsStatus[] statuses = journal.getUfs().listStatus(journal.getCheckpointDir().toString());
    if (statuses != null) {
      for (UfsStatus status : statuses) {
        UfsJournalFile file = UfsJournalFile.decodeCheckpointFile(journal, status.getName());
        if (file != null) {
          checkpoints.add(file);
        }
      }
      Collections.sort(checkpoints);
    }
    if (checkpoints.isEmpty()) {
      return 0;
    }
    return checkpoints.get(checkpoints.size() - 1).getEnd();
  }
}

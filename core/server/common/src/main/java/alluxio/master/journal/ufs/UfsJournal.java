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
import alluxio.PropertyKey;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalReader;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.URIUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UFS-based journal.
 *
 * The journal is made up of 2 components:
 * - The checkpoint: the full state of the master
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * Journal file structure:
 * journal_folder/version/logs/StartSequenceNumber-EndSequenceNumber
 * journal_folder/version/checkpoints/0-EndSequenceNumber
 * journal_folder/version/.tmp/master_id (master_id can be transformed(worker_rpc_net_address))
 */
@ThreadSafe
public class UfsJournal implements Journal {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournal.class);
  public static final long UNKNOWN_SEQUENCE_NUMBER = Long.MAX_VALUE;

  /** The journal version. */
  private static final String VERSION = "v1";

  private static final String LOG_DIRNAME = "logs";
  private static final String CHECKPOINT_DIRNAME = "checkpoints";
  private static final String TMP_DIRNAME = ".tmp";

  private final URI mLogDir;
  private final URI mCheckpointDir;
  private final URI mTmpDir;

  private final CreateOptions mOptions;

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  static class JournalFile implements Comparable<JournalFile> {
    /** The location of the file. */
    final URI mLocation;
    final long mStart;
    final long mEnd;

    JournalFile(URI location, long start, long end) {
      mLocation = location;
      mStart = start;
      mEnd = end;
    }

    boolean isIncompleteLog() {
      return mStart != UNKNOWN_SEQUENCE_NUMBER && mEnd == UNKNOWN_SEQUENCE_NUMBER;
    }

    boolean isTmpCheckpoint() {
      return mStart == UNKNOWN_SEQUENCE_NUMBER && mEnd == UNKNOWN_SEQUENCE_NUMBER;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("location", mLocation).add("start", mStart)
          .add("end", mEnd).toString();
    }

    @Override
    public int compareTo(JournalFile other) {
       long diff = mEnd - other.mEnd;
      if (diff < 0) {
        return -1;
      } else if (diff == 0) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  static class Snapshot {
    final List<JournalFile> mCheckpoints;
    final List<JournalFile> mLogs;
    final List<JournalFile> mTemporaryCheckpoints;

    Snapshot(List<JournalFile> checkpoints, List<JournalFile> logs,
        List<JournalFile> temporaryCheckpoints) {
      mCheckpoints = checkpoints;
      mLogs = logs;
      mTemporaryCheckpoints = temporaryCheckpoints;
    }
  }

  public Snapshot getSnapshot() throws IOException {
    // Checkpoints.
    UnderFileStatus[] statuses = mUfs.listStatus(getCheckpointDir().toString());
    List<JournalFile> checkpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      checkpoints.add(decodeCheckpointOrLogFile(status.getName(), true  /* is_checkpoint */));
    }
    Collections.sort(checkpoints);

    statuses = mUfs.listStatus(getLogDir().toString());
    List<JournalFile> logs = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      logs.add(decodeCheckpointOrLogFile(status.getName(), false  /* is_checkpoint */));
    }
    Collections.sort(logs);
    // The secondary masters do not read incomplete logs.
    if (!mOptions.getPrimary() && !logs.isEmpty()) {
      if (logs.get(logs.size() - 1).isIncompleteLog()) {
        logs.remove(logs.size() - 1);
      }
    }

    statuses = mUfs.listStatus(getTmpDir().toString());
    List<JournalFile> tmpCheckpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      tmpCheckpoints.add(decodeTmpFile(status.getName()));
    }

    return new Snapshot(checkpoints, logs, tmpCheckpoints);
  }

  public JournalFile getCurrentLog() throws IOException {
    UnderFileStatus[] statuses = mUfs.listStatus(getLogDir().toString());
    List<JournalFile> logs = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      logs.add(decodeCheckpointOrLogFile(status.getName(), false  /* is_checkpoint */));
    }
    JournalFile file =  Collections.max(logs);
    if (file.isIncompleteLog()) {
      return file;
    }
    return null;
  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location, CreateOptions options) {
    mLocation = URIUtils.appendPathOrDie(location, VERSION);
    mUfs = UnderFileSystem.Factory.get(mLocation.toString());

    mLogDir = URIUtils.appendPathOrDie(mLocation, LOG_DIRNAME);
    mCheckpointDir = URIUtils.appendPathOrDie(mLocation, CHECKPOINT_DIRNAME);
    mTmpDir = URIUtils.appendPathOrDie(mLocation, TMP_DIRNAME);
    mOptions = options;
  }

  public URI getLogDir() {
    return mLogDir;
  }

  public URI getCheckpointDir() {
    return mCheckpointDir;
  }

  public URI getTmpDir() {
    return mTmpDir;
  }

  public UnderFileSystem getUfs() {
    return mUfs;
  }

  public CreateOptions getOptions() {
    return mOptions;
  }

  @Override
  public URI getLocation() {
    return mLocation;
  }

  @Override
  public JournalReader getReader() {
    return new UfsJournalReader(this);
  }


  @Override
  public boolean isFormatted() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(mLocation.toString());
    UnderFileStatus[] files = ufs.listStatus(mLocation.toString());
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (UnderFileStatus file : files) {
      if (file.getName().startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }

  public URI encodeCheckpointOrLogFileLocation(long start, long end, boolean isCheckpoint) {
    String filename = String.format("%0xd-%0xd", start, end);
    URI location =
        URIUtils.appendPathOrDie(isCheckpoint ? getCheckpointDir() : getLogDir(), filename);
    return location;
  }

  private JournalFile decodeCheckpointOrLogFile(String filename, boolean isCheckpoint) {
    URI location =
        URIUtils.appendPathOrDie(isCheckpoint ? getCheckpointDir() : getLogDir(), filename);
    try {
      String[] parts = filename.split("-");
      Preconditions.checkState(parts.length == 2);
      long start = Long.decode(parts[0]);
      long end = Long.decode(parts[1]);
      return new JournalFile(location, start, end);
    } catch (IllegalStateException | NumberFormatException e) {
      LOG.error("Illegal journal file {}.", location);
      throw e;
    }
  }

  private JournalFile decodeTmpFile(String filename) {
    URI location = URIUtils.appendPathOrDie(getTmpDir(), filename);
    long start = UNKNOWN_SEQUENCE_NUMBER;
    long end = UNKNOWN_SEQUENCE_NUMBER;
    return new JournalFile(location, start, end);
  }
}

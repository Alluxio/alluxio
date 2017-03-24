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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
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

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  static class JournalFile implements Comparable<JournalFile> {
    /** The location of the file. */
    final URI mLocation;
    final long mStart;
    final long mEnd;
    final long mLastModifiedTime;

    JournalFile(URI location, long start, long end, long lastModifiedTime) {
      mLocation = location;
      mStart = start;
      mEnd = end;
      mLastModifiedTime = lastModifiedTime;
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
    final JournalFile mCheckpoint;
    final List<JournalFile> mTemporaryCheckpoints;

    final JournalFile mCurrentLog;
    final List<JournalFile> mCompletedLogs;

    Snapshot(JournalFile checkpoint, List<JournalFile> temporaryCheckpoints, JournalFile currentLog,
        List<JournalFile> completedLogs) {
      mCheckpoint = checkpoint;
      mTemporaryCheckpoints = temporaryCheckpoints;
      mCurrentLog = currentLog;
      mCompletedLogs = completedLogs;
    }
  }

  public Snapshot snapshot() {

  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location) {
    mLocation = URIUtils.appendPathOrDie(location, VERSION);
  }

  private URI getLogDirName() {
    return URIUtils.appendPathOrDie(mLocation, LOG_DIRNAME);
  }

  private URI getCheckpointDirName() {
    return URIUtils.appendPathOrDie(mLocation, CHECKPOINT_DIRNAME);
  }

  private URI getTmpDirName() {
    return URIUtils.appendPathOrDie(mLocation, TMP_DIRNAME);
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

  private JournalFile parserCheckpointOrLogFile(String filename, long lastModificationTime,
      boolean isCheckpoint) {
    URI location =
        URIUtils.appendPathOrDie(isCheckpoint ? getCheckpointDirName() : getLogDirName(), filename);
    String[] parts = filename.split("-");
    Preconditions.checkState(parts.length == 2);
    long start = Long.decode(parts[0]);
    long end = Long.decode(parts[1]);

    return new JournalFile(location, start, end, lastModificationTime);
  }

  private JournalFile parseTmpFile(String filename, long lastModificationTime) {
    URI location = URIUtils.appendPathOrDie(getTmpDirName(), filename);
    long start = UNKNOWN_SEQUENCE_NUMBER;
    long end = UNKNOWN_SEQUENCE_NUMBER;
    return new JournalFile(location, start, end, lastModificationTime);
  }
}

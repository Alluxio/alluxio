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
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalReaderCreateOptions;
import alluxio.master.journal.options.JournalWriterCreateOptions;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.URIUtils;
import alluxio.util.UnderFileSystemUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UFS-based journal.
 *
 * The journal is made up of 2 components:
 * - The checkpoint: the full state of the master.
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * Journal file structure:
 * journal_folder/version/logs/StartSequenceNumber-EndSequenceNumber
 * journal_folder/version/checkpoints/0-EndSequenceNumber
 * journal_folder/version/.tmp/random_id
 */
@ThreadSafe
public class UfsJournal implements Journal {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournal.class);
  public static final long UNKNOWN_SEQUENCE_NUMBER = Long.MAX_VALUE;
  /** The journal version. */
  public static final String VERSION = "v1";

  /** Directory for journal edit logs including the incomplete log file. */
  private static final String LOG_DIRNAME = "logs";
  /** Directory for committed checkpoints. */
  private static final String CHECKPOINT_DIRNAME = "checkpoints";
  /** Directory for temporary files. */
  private static final String TMP_DIRNAME = ".tmp";

  private final URI mLogDir;
  private final URI mCheckpointDir;
  private final URI mTmpDir;

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location) {
    mLocation = URIUtils.appendPathOrDie(location, VERSION);
    mUfs = UnderFileSystem.Factory.get(mLocation.toString());

    mLogDir = URIUtils.appendPathOrDie(mLocation, LOG_DIRNAME);
    mCheckpointDir = URIUtils.appendPathOrDie(mLocation, CHECKPOINT_DIRNAME);
    mTmpDir = URIUtils.appendPathOrDie(mLocation, TMP_DIRNAME);
  }

  @Override
  public URI getLocation() {
    return mLocation;
  }

  @Override
  public JournalReader getReader(JournalReaderCreateOptions options) {
    return new UfsJournalReader(this, options);
  }

  @Override
  public JournalWriter getWriter(JournalWriterCreateOptions options) throws IOException {
    if (options.getPrimary()) {
      return new UfsJournalLogWriter(this, options);
    } else {
      return new UfsJournalCheckpointWriter(this, options);
    }
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

  @Override
  public void format() throws IOException {
    URI location = getLocation();
    LOG.info("Formatting {}", location);
    if (mUfs.isDirectory(location.toString())) {
      for (UnderFileStatus p : mUfs.listStatus(location.toString())) {
        URI childPath = URIUtils.appendPathOrDie(location, p.getName());
        boolean failedToDelete;
        if (p.isDirectory()) {
          failedToDelete = !mUfs.deleteDirectory(childPath.toString(),
              DeleteOptions.defaults().setRecursive(true));
        } else {
          failedToDelete = !mUfs.deleteFile(childPath.toString());
        }
        if (failedToDelete) {
          throw new IOException(String.format("Failed to delete %s", childPath));
        }
      }
    } else if (!mUfs.mkdirs(location.toString())) {
      throw new IOException(String.format("Failed to create %s", location));
    }

    // Create a breadcrumb that indicates that the journal folder has been formatted.
    UnderFileSystemUtils.touch(URIUtils.appendPathOrDie(location,
        Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX) + System.currentTimeMillis())
        .toString());
  }

  /**
   * @return the log directory location
   */
  public URI getLogDir() {
    return mLogDir;
  }

  /**
   * @return the checkpoint directory location
   */
  public URI getCheckpointDir() {
    return mCheckpointDir;
  }

  /**
   * @return the temporary directory location
   */
  public URI getTmpDir() {
    return mTmpDir;
  }

  /**
   * @return the under file system instance
   */
  public UnderFileSystem getUfs() {
    return mUfs;
  }

  /**
   * Creates a checkpoint location under the checkpoint directory.
   *
   * @param end the end sequence number (exclusive)
   * @return the location
   */
  public URI encodeCheckpointFileLocation(long end) {
    String filename = String.format("%x-%x", 0, end);
    URI location = URIUtils.appendPathOrDie(getCheckpointDir(), filename);
    return location;
  }

  /**
   * Creates a log location under the log directory.
   *
   * @param start the start sequence number (inclusive)
   * @param end the end sequence number (exclusive)
   * @return the location
   */
  public URI encodeLogFileLocation(long start, long end) {
    String filename = String.format("%x-%x", start, end);
    URI location = URIUtils.appendPathOrDie(getLogDir(), filename);
    return location;
  }

  /**
   * Creates a temporary location under the temporary directory.
   *
   * @return the location
   */
  public URI encodeTemporaryCheckpointFileLocation() {
    return URIUtils.appendPathOrDie(getTmpDir(), UUID.randomUUID().toString());
  }

  /**
   * Decodes a checkpoint or a log file name into a {@link UfsJournalFile}.
   *
   * @param filename the filename
   * @param isCheckpoint whether this is a checkpoint file or a log file
   * @return the instance of {@link UfsJournalFile}
   */
  public UfsJournalFile decodeCheckpointOrLogFile(String filename, boolean isCheckpoint) {
    URI location =
        URIUtils.appendPathOrDie(isCheckpoint ? getCheckpointDir() : getLogDir(), filename);
    try {
      String[] parts = filename.split("-");
      Preconditions.checkState(parts.length == 2);
      long start = Long.decode(parts[0]);
      long end = Long.decode(parts[1]);
      if (isCheckpoint) {
        Preconditions.checkState(start == 0);
        return UfsJournalFile.createCheckpointFile(location, end);
      } else {
        return UfsJournalFile.createLogFile(location, start, end);
      }
    } catch (IllegalStateException | NumberFormatException e) {
      LOG.error("Illegal journal file {}.", location);
      throw e;
    }
  }

  /**
   * Decodes a temporary checkpoint file name into a {@link UfsJournalFile}.
   *
   * @param filename the temporary checkpoint file name
   * @return the instance of {@link UfsJournalFile}
   */
  public UfsJournalFile decodeTemporaryCheckpointFile(String filename) {
    URI location = URIUtils.appendPathOrDie(getTmpDir(), filename);
    return UfsJournalFile.createTmpCheckpointFile(location);
  }

  /**
   * A snapshot of everything in the journal.
   */
  static class Snapshot {
    /** The committed checkpoints. */
    final List<UfsJournalFile> mCheckpoints;
    /** The journal edit logs including the incomplete log. */
    final List<UfsJournalFile> mLogs;
    /** The temporary checkpoint files. */
    final List<UfsJournalFile> mTemporaryCheckpoints;

    /**
     * Creates an instance of the journal snapshot.
     *
     * @param checkpoints the checkpoints
     * @param logs the logs including the incomplete log
     * @param temporaryCheckpoints the temporary checkpoint files
     */
    Snapshot(List<UfsJournalFile> checkpoints, List<UfsJournalFile> logs,
        List<UfsJournalFile> temporaryCheckpoints) {
      mCheckpoints = checkpoints;
      mLogs = logs;
      mTemporaryCheckpoints = temporaryCheckpoints;
    }
  }

  /**
   * Creates a snapshot of the journal.
   *
   * @return the journal snapshot
   * @throws IOException if any I/O errors occur
   */
  public Snapshot getSnapshot() throws IOException {
    // Checkpoints.
    UnderFileStatus[] statuses = mUfs.listStatus(getCheckpointDir().toString());
    List<UfsJournalFile> checkpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      checkpoints.add(decodeCheckpointOrLogFile(status.getName(), true  /* is_checkpoint */));
    }
    Collections.sort(checkpoints);

    statuses = mUfs.listStatus(getLogDir().toString());
    List<UfsJournalFile> logs = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      logs.add(decodeCheckpointOrLogFile(status.getName(), false  /* is_checkpoint */));
    }
    Collections.sort(logs);

    statuses = mUfs.listStatus(getTmpDir().toString());
    List<UfsJournalFile> tmpCheckpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      tmpCheckpoints.add(decodeTemporaryCheckpointFile(status.getName()));
    }

    return new Snapshot(checkpoints, logs, tmpCheckpoints);
  }

  /**
   * Gets the current log (the incomplete log) that is being written to.
   *
   * @return the current log
   * @throws IOException if any I/O errors occur
   */
  public UfsJournalFile getCurrentLog() throws IOException {
    UnderFileStatus[] statuses = mUfs.listStatus(getLogDir().toString());
    List<UfsJournalFile> logs = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      logs.add(decodeCheckpointOrLogFile(status.getName(), false  /* is_checkpoint */));
    }
    UfsJournalFile file =  Collections.max(logs);
    if (file.isIncompleteLog()) {
      return file;
    }
    return null;
  }

  /**
   * Gets the first journal log sequence number that is not yet checkpointed.
   *
   * @return the first journal log sequence number that is not yet checkpointed
   * @throws IOException if any I/O errors occur
   */
  public long getNextLogSequenceToCheckpoint() throws IOException {
    UnderFileStatus[] statuses = mUfs.listStatus(getCheckpointDir().toString());
    List<UfsJournalFile> checkpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      checkpoints.add(decodeCheckpointOrLogFile(status.getName(), true  /* is_checkpoint */));
    }
    Collections.sort(checkpoints);
    if (checkpoints.isEmpty()) {
      return 0;
    }
    return checkpoints.get(checkpoints.size() - 1).getEnd();
  }
}

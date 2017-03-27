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
import alluxio.master.journal.JournalCreateOptions;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.JournalWriterCreateOptions;
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
 * journal_folder/version/.tmp/random_id
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

  private final JournalCreateOptions mOptions;

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The UFS where the journal is being written to. */
  private final UnderFileSystem mUfs;

  static class Snapshot {
    final List<UfsJournalFile> mCheckpoints;
    final List<UfsJournalFile> mLogs;
    final List<UfsJournalFile> mTemporaryCheckpoints;

    Snapshot(List<UfsJournalFile> checkpoints, List<UfsJournalFile> logs,
        List<UfsJournalFile> temporaryCheckpoints) {
      mCheckpoints = checkpoints;
      mLogs = logs;
      mTemporaryCheckpoints = temporaryCheckpoints;
    }
  }

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
    // The secondary masters do not read incomplete logs.
    if (!mOptions.getPrimary() && !logs.isEmpty()) {
      if (logs.get(logs.size() - 1).isIncompleteLog()) {
        logs.remove(logs.size() - 1);
      }
    }

    statuses = mUfs.listStatus(getTmpDir().toString());
    List<UfsJournalFile> tmpCheckpoints = new ArrayList<>();
    for (UnderFileStatus status : statuses) {
      tmpCheckpoints.add(decodeTemporaryCheckpointFile(status.getName()));
    }

    return new Snapshot(checkpoints, logs, tmpCheckpoints);
  }

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
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location, JournalCreateOptions options) {
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

  public JournalCreateOptions getOptions() {
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

  public URI getCheckpointOrLogFileLocation(long start, long end, boolean isCheckpoint) {
    String filename = String.format("%0xd-%0xd", start, end);
    URI location =
        URIUtils.appendPathOrDie(isCheckpoint ? getCheckpointDir() : getLogDir(), filename);
    return location;
  }

  public URI getTemporaryCheckpointFileLocation() {
    return URIUtils.appendPathOrDie(getTmpDir(), UUID.randomUUID().toString());
  }

  private UfsJournalFile decodeCheckpointOrLogFile(String filename, boolean isCheckpoint) {
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
        return UfsJournalFile.createLog(location, start, end);
      }
    } catch (IllegalStateException | NumberFormatException e) {
      LOG.error("Illegal journal file {}.", location);
      throw e;
    }
  }

  private UfsJournalFile decodeTemporaryCheckpointFile(String filename) {
    URI location = URIUtils.appendPathOrDie(getTmpDir(), filename);
    return UfsJournalFile.createTmpCheckpoint(location);
  }

  @Override
  public void format() throws IOException {
    URI location = getLocation();
    LOG.info("Formatting {}", location);
    UnderFileSystem ufs = UnderFileSystem.Factory.get(location.toString());
    if (ufs.isDirectory(location.toString())) {
      for (UnderFileStatus p : ufs.listStatus(location.toString())) {
        URI childPath;
        try {
          childPath = URIUtils.appendPath(location, p.getName());
        } catch (URISyntaxException e) {
          throw new RuntimeException(e.getMessage());
        }
        boolean failedToDelete;
        if (p.isDirectory()) {
          failedToDelete = !ufs.deleteDirectory(childPath.toString(),
              DeleteOptions.defaults().setRecursive(true));
        } else {
          failedToDelete = !ufs.deleteFile(childPath.toString());
        }
        if (failedToDelete) {
          throw new IOException(String.format("Failed to delete %s", childPath));
        }
      }
    } else if (!ufs.mkdirs(location.toString())) {
      throw new IOException(String.format("Failed to create %s", location));
    }

    // Create a breadcrumb that indicates that the journal folder has been formatted.
    try {
      UnderFileSystemUtils.touch(URIUtils.appendPath(location,
          Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX) + System.currentTimeMillis())
          .toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public JournalWriter getWriter(JournalWriterCreateOptions options) throws IOException {
    if (mOptions.getPrimary()) {
      return new UfsJournalLogWriter(this, options);
    } else {
      return new UfsJournalCheckpointWriter(this, options);
    }
  }
}

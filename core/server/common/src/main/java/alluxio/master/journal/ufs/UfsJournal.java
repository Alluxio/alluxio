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
import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.master.journal.options.JournalWriterOptions;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.URIUtils;
import alluxio.util.UnderFileSystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UFS-based journal.
 *
 * The journal is made up of 2 components:
 * - The checkpoint:  a snapshot of the master state
 * - The log entries: incremental entries to apply to the checkpoint.
 *
 * The journal log entries must be self-contained. Checkpoint is considered as a compaction of
 * a set of journal log entries. If the master does not do any checkpoint, the journal should
 * still be sufficient.
 *
 * Journal file structure:
 * journal_folder/version/logs/StartSequenceNumber-EndSequenceNumber
 * journal_folder/version/checkpoints/0-EndSequenceNumber
 * journal_folder/version/.tmp/random_id
 */
@ThreadSafe
public class UfsJournal implements Journal {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournal.class);
  /**
   * This is set to Long.MAX_VALUE such that the current log can be sorted after any other
   * completed logs.
   */
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
   * @return the ufs configuration to use for the journal operations
   */
  protected static UnderFileSystemConfiguration getJournalUfsConf() {
    Map<String, String> ufsConf =
        Configuration.getNestedProperties(PropertyKey.MASTER_JOURNAL_UFS_OPTION);
    return UnderFileSystemConfiguration.defaults().setUserSpecifiedConf(ufsConf);
  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location) {
    this(location, UnderFileSystem.Factory.create(location.toString(), getJournalUfsConf()));
  }

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   * @param ufs the under file system
   */
  UfsJournal(URI location, UnderFileSystem ufs) {
    mLocation = URIUtils.appendPathOrDie(location, VERSION);
    mUfs = ufs;

    mLogDir = URIUtils.appendPathOrDie(mLocation, LOG_DIRNAME);
    mCheckpointDir = URIUtils.appendPathOrDie(mLocation, CHECKPOINT_DIRNAME);
    mTmpDir = URIUtils.appendPathOrDie(mLocation, TMP_DIRNAME);
  }

  @Override
  public URI getLocation() {
    return mLocation;
  }

  @Override
  public JournalReader getReader(JournalReaderOptions options) {
    return new UfsJournalReader(this, options);
  }

  @Override
  public JournalWriter getWriter(JournalWriterOptions options) throws IOException {
    if (options.isPrimary()) {
      return new UfsJournalLogWriter(this, options);
    } else {
      return new UfsJournalCheckpointWriter(this, options);
    }
  }

  @Override
  public long getNextSequenceNumberToCheckpoint() throws IOException {
    return UfsJournalSnapshot.getNextLogSequenceNumberToCheckpoint(this);
  }

  @Override
  public boolean isFormatted() throws IOException {
    UfsStatus[] files = mUfs.listStatus(mLocation.toString());
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (UfsStatus file : files) {
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
      for (UfsStatus status : mUfs.listStatus(location.toString())) {
        String childPath = URIUtils.appendPathOrDie(location, status.getName()).toString();
        if (status.isDirectory()
            && !mUfs.deleteDirectory(childPath, DeleteOptions.defaults().setRecursive(true))
            || status.isFile() && !mUfs.deleteFile(childPath)) {
          throw new IOException(String.format("Failed to delete %s", childPath));
        }
      }
    } else if (!mUfs.mkdirs(location.toString())) {
      throw new IOException(String.format("Failed to create %s", location));
    }

    // Create a breadcrumb that indicates that the journal folder has been formatted.
    UnderFileSystemUtils.touch(mUfs, URIUtils.appendPathOrDie(location,
        Configuration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX) + System.currentTimeMillis())
        .toString());
  }

  /**
   * @return the log directory location
   */
  URI getLogDir() {
    return mLogDir;
  }

  /**
   * @return the checkpoint directory location
   */
  URI getCheckpointDir() {
    return mCheckpointDir;
  }

  /**
   * @return the temporary directory location
   */
  URI getTmpDir() {
    return mTmpDir;
  }

  /**
   * @return the under file system instance
   */
  UnderFileSystem getUfs() {
    return mUfs;
  }
}

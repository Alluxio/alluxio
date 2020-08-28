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

package alluxio.master.journalv0.ufs;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.journalv0.Journal;
import alluxio.master.journalv0.JournalFormatter;
import alluxio.master.journalv0.JournalReader;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.URIUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of UFS-based journal.
 *
 * The journal is made up of 2 components:
 * - The checkpoint: the full state of the master
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * To construct the full state of the master, all the entries must be applied to the checkpoint in
 * order. The entry file most recently being written to is in the base journal folder, where the
 * completed entry files are in the "completed" folder.
 */
@ThreadSafe
public class UfsJournal implements Journal {
  /** The log number for the first completed log. */
  protected static final long FIRST_COMPLETED_LOG_NUMBER = 1L;
  /** The folder for completed logs. */
  private static final String COMPLETED_LOCATION = "completed";
  /** The file extension for the current log file. */
  private static final String CURRENT_LOG_EXTENSION = ".out";
  /** The file name of the checkpoint file. */
  private static final String CHECKPOINT_FILENAME = "checkpoint.data";
  /** The base of the entry log file names, without the file extension. */
  private static final String ENTRY_LOG_FILENAME_BASE = "log";
  /** The location where this journal is stored. */
  protected final URI mLocation;
  /** The formatter for this journal. */
  private final JournalFormatter mJournalFormatter;

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location) {
    mLocation = location;
    mJournalFormatter = JournalFormatter.Factory.create();
  }

  /**
   * @return the location of the completed logs
   */
  public URI getCompletedLocation() {
    try {
      return URIUtils.appendPath(mLocation, COMPLETED_LOCATION);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the location of the journal checkpoint
   */
  protected URI getCheckpoint() {
    try {
      return URIUtils.appendPath(mLocation, CHECKPOINT_FILENAME);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the location of the current log
   */
  public URI getCurrentLog() {
    try {
      return URIUtils.appendPath(mLocation, ENTRY_LOG_FILENAME_BASE + CURRENT_LOG_EXTENSION);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param logNumber the log number to get the path for
   * @return the location of the completed log for a particular log number
   */
  protected URI getCompletedLog(long logNumber) {
    try {
      return URIUtils.appendPath(getCompletedLocation(),
          String.format("%s.%020d", ENTRY_LOG_FILENAME_BASE, logNumber));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the {@link JournalFormatter} for this journal
   */
  protected JournalFormatter getJournalFormatter() {
    return mJournalFormatter;
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
    UfsStatus[] files;
    try (UnderFileSystem ufs = UnderFileSystem.Factory.create(mLocation.toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()))) {
      files = ufs.listStatus(mLocation.toString());
    }

    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = ServerConfiguration.get(PropertyKey.MASTER_FORMAT_FILE_PREFIX);
    for (UfsStatus file : files) {
      if (file.getName().startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }
}

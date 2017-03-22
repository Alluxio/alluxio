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
import alluxio.master.journal.JournalFormatter;
import alluxio.master.journal.JournalReader;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.URIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournal.class);

  /** The journal version. */
  private static final String VERSION = "v1";

  /** The folder for completed logs. */
  private static final String COMPLETED_LOG_DIRNAME = "logs";
  private static final String CHECKPOINT_DIRNAME = "checkpoints";
  private static final String TMP_DIRNAME = ".tmp";
  private static final String CURRENT_LOG_FILENAME = "log.out";

  /** The location where this journal is stored. */
  private final URI mLocation;
  /** The formatter for this journal. */
  private final JournalFormatter mJournalFormatter;

  /**
   * Creates a new instance of {@link UfsJournal}.
   *
   * @param location the location for this journal
   */
  public UfsJournal(URI location) {
    try {
      mLocation = URIUtils.appendPath(location, VERSION);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    mJournalFormatter = JournalFormatter.Factory.create();
  }

  public URI getCompletedLogDirName() {
    try {
      return URIUtils.appendPath(mLocation, COMPLETED_LOG_DIRNAME);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public URI getCheckpointDirName() {
    try {
      return URIUtils.appendPath(mLocation, CHECKPOINT_DIRNAME);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public URI getTmpDirName() {
    try {
      return URIUtils.appendPath(mLocation, TMP_DIRNAME);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public URI getCurrentLogFileName() {
    try {
      return URIUtils.appendPath(mLocation, CURRENT_LOG_FILENAME);
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
}

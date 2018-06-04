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

package alluxio.master;

import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.collect.Maps;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Manages creating and restoring from backups.
 *
 * Journals are written as a series of entries in gzip format.
 */
public class BackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupManager.class);

  private final MasterRegistry mRegistry;

  /**
   * @param registry a master registry containing the masters to backup or restore
   */
  public BackupManager(MasterRegistry registry) {
    mRegistry = registry;
  }

  /**
   * Writes a backup to the specified stream.
   *
   * @param os the stream to write to
   */
  public void backup(OutputStream os) throws IOException {
    int count = 0;
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(os);
    for (Master master : mRegistry.getServers()) {
      Iterator<JournalEntry> it = master.getJournalEntryIterator();
      while (it.hasNext()) {
        it.next().toBuilder().clearSequenceNumber().build().writeDelimitedTo(zipStream);
        count++;
      }
    }
    // finish() instead of close() since close would close os, which is owned by the caller.
    zipStream.finish();
    LOG.info("Created backup with {} entries", count);
  }

  /**
   * Restores master state from the specified backup.
   *
   * @param is an input stream to read from the backup
   */
  public void initFromBackup(InputStream is) throws IOException {
    int count = 0;
    try (GzipCompressorInputStream gzIn = new GzipCompressorInputStream(is);
         JournalEntryStreamReader reader = new JournalEntryStreamReader(gzIn)) {
      List<Master> masters = mRegistry.getServers();
      JournalEntry entry;
      Map<String, Master> mastersByName = Maps.uniqueIndex(masters, Master::getName);
      while ((entry = reader.readEntry()) != null) {
        String masterName = JournalEntryAssociation.getMasterForEntry(entry);
        Master master = mastersByName.get(masterName);
        master.processJournalEntry(entry);
        try (JournalContext jc = master.createJournalContext()) {
          jc.append(entry);
          count++;
        }
      }
    }
    LOG.info("Imported {} entries from backup", count);
  }
}

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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.Maps;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Manages creating and restoring from backups.
 *
 * Journals are written as a series of entries in gzip format.
 */
public class BackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupManager.class);

  /**
   * The name format of the backup files (E.g., alluxio-backup-2018-12-14-1544749081561.gz).
   * The first value is the current local date of UTC time zone. The second value is
   * the number of milliseconds from the epoch of 1970-01-01T00:00:00Z to the current time.
   */
  public static final String BACKUP_FILE_FORMAT = "alluxio-backup-%s-%s.gz";
  public static final Pattern BACKUP_FILE_PATTERN
      = Pattern.compile("alluxio-backup-[0-9]+-[0-9]+-[0-9]+-([0-9]+).gz");

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
   * @param entryCount will receive total entry count that are backed up
   */
  public void backup(OutputStream os, AtomicLong entryCount) throws IOException {
    // Create gZIP compressed stream as back-up stream.
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(os);
    for (Master master : mRegistry.getServers()) {
      Iterator<JournalEntry> it = master.getJournalEntryIterator();
      while (it.hasNext()) {
        it.next().toBuilder().clearSequenceNumber().build().writeDelimitedTo(zipStream);
        entryCount.incrementAndGet();
      }
    }
    // finish() instead of close() since close would close os, which is owned by the caller.
    zipStream.finish();
    LOG.info("Created backup with {} entries", entryCount.get());
  }

  /**
   * Restores master state from the specified backup.
   *
   * @param is an input stream to read from the backup
   */
  public void initFromBackup(InputStream is) throws IOException {
    AtomicLong appliedEntryCount = new AtomicLong(0);
    // Progress executor
    ScheduledExecutorService traceExecutor = Executors.newScheduledThreadPool(1,
        ThreadFactoryUtils.build("master-backup-tracer-%d", true));
    traceExecutor.scheduleAtFixedRate(() -> {
      LOG.info("{} entries from backup applied so far...", appliedEntryCount.get());
    }, 30, 30, TimeUnit.SECONDS);

    try (GzipCompressorInputStream gzIn = new GzipCompressorInputStream(is);
         JournalEntryStreamReader reader = new JournalEntryStreamReader(gzIn)) {
      List<Master> masters = mRegistry.getServers();
      Map<String, Master> mastersByName = Maps.uniqueIndex(masters, Master::getName);
      // Create buffer to avoid flushing per-entry.
      int bufferLimit = Configuration.getInt(PropertyKey.MASTER_BACKUP_ENTRY_BATCH_SIZE);
      List<JournalEntry> entryBuffer = new ArrayList<>(bufferLimit);
      // Apply entries from input stream.
      JournalEntry entry;
      while (true) {
        // Read next entry.
        entry = reader.readEntry();
        // Buffer next entry.
        if (entry != null) {
          entryBuffer.add(entry);
        }
        // Process the buffer if it is full or the input stream has no more entries.
        if (entry == null || entryBuffer.size() >= bufferLimit) {
          // Used to keep track of journal contexts per master.
          Map<String, JournalContext> contextMap = new HashMap<>();
          // Process each buffered entry.
          for (JournalEntry bufferedEntry : entryBuffer) {
            String masterName = JournalEntryAssociation.getMasterForEntry(bufferedEntry);
            Master master = mastersByName.get(masterName);
            // Apply entry to master.
            master.processJournalEntry(bufferedEntry);
            // Create journal context if master is seens the first time.
            if (!contextMap.containsKey(masterName)) {
              contextMap.put(masterName, master.createJournalContext());
            }
            // Apply entry to journal.
            contextMap.get(masterName).append(bufferedEntry);
            // entry is processed.
            appliedEntryCount.incrementAndGet();
          }
          // Close journal context.
          for (JournalContext context : contextMap.values()) {
            context.close();
          }
          // Clear the buffer.
          entryBuffer.clear();
        }
        // Quit applying when stream has no more entries.
        if (entry == null) {
          break;
        }
      }
    } finally {
      traceExecutor.shutdownNow();
    }
    LOG.info("Restored {} entries from backup", appliedEntryCount.get());
  }
}

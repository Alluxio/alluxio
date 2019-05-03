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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
   */
  public void backup(OutputStream os) throws IOException {
    // Create gZIP compressed stream as back-up stream.
    GzipCompressorOutputStream zipStream = new GzipCompressorOutputStream(os);

    // Executor for writing backup content.
    ExecutorService backupWriterExecutor =
        Executors.newSingleThreadExecutor(ThreadFactoryUtils.build("backup-writer-%d", true));
    // Executor for reading master state.
    ExecutorService backupMasterReaderExecutor = Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("backup-master-reader-%d", true));

    // Entry queue will be used as a buffer and synchronization between readers and writer.
    // Use of {@link LinkedBlockingQueue} is preferred becaue of {@code #drainTo()} method,
    // using which all existing entries can be drained while allowing writes.
    // Processing/draining one-by-one using {@link ConcurrentLinkedQueue} proved to be
    // inefficient compared to draining with dedicated method.
    LinkedBlockingQueue<JournalEntry> journalEntryQueue = new LinkedBlockingQueue<>(
        ServerConfiguration.getInt(PropertyKey.MASTER_BACKUP_ENTRY_BUFFER_COUNT));

    // Shows how many entries have been written.
    AtomicLong writtenEntryCount = new AtomicLong(0);

    // Futures for reader/writer tasks.
    SettableFuture<Void> readerTaskFuture = SettableFuture.create();
    SettableFuture<Void> writerTaskFuture = SettableFuture.create();

    // Submit master reader task.
    backupMasterReaderExecutor.submit(() -> {
      try {
        for (Master master : mRegistry.getServers()) {
          Iterator<JournalEntry> it = master.getJournalEntryIterator();
          if (it != null) {
            while (!writerTaskFuture.isDone() && it.hasNext()) {
              JournalEntry je = it.next();
              // Try to push the entry as long as writer is alive.
              while (!writerTaskFuture.isDone()) {
                if (journalEntryQueue.offer(je, 100, TimeUnit.MILLISECONDS)) {
                  break;
                }
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        // Store exception.
        readerTaskFuture.setException(ie);
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while reading master state.", ie);
      } catch (Exception exc) {
        // Store exception.
        readerTaskFuture.setException(exc);
      } finally {
        readerTaskFuture.set(null);
      }
    });
    // Will shutdown as soon as the pending tasks are completed.
    backupMasterReaderExecutor.shutdown();

    // Submit writer task.
    backupWriterExecutor.submit(() -> {
      try {
        List<JournalEntry> pendingEntries = new LinkedList<>();

        while (!readerTaskFuture.isDone() || journalEntryQueue.size() > 0) {
          // Drain pending entries.
          pendingEntries.clear();
          if (0 == journalEntryQueue.drainTo(pendingEntries)) {
            // No elements at the moment. Fall back to polling.
            /**
             * Need to poll with a timeout here because: Entries for a reader might be written
             * already or it might not have an entry. In that case we will still go in here, without
             * any future entry in the queue.
             */
            JournalEntry entry = journalEntryQueue.poll(100, TimeUnit.MILLISECONDS);
            if (entry == null) {
              // No entry yet.
              continue;
            }
            pendingEntries.add(entry);
          }
          // Write entries to back-up stream.
          for (JournalEntry je : pendingEntries) {
            je.writeDelimitedTo(zipStream);
            writtenEntryCount.incrementAndGet();
          }
          pendingEntries.clear();
        }
      } catch (InterruptedException ie) {
        // Store exception.
        writerTaskFuture.setException(ie);
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while writing to backup stream.", ie);
      } catch (Exception exc) {
        // Store exception.
        writerTaskFuture.setException(exc);
      } finally {
        writerTaskFuture.set(null);
      }
    });
    // Will shutdown as soon as the pending tasks are completed.
    backupWriterExecutor.shutdown();

    // Wait until backup is done.
    try {
      // Wait for master reader.
      readerTaskFuture.get();
      // Wait for backup writer.
      writerTaskFuture.get();
    } catch (InterruptedException ie) {
      // Continue interrupt chain.
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted while waiting for backup threads.", ie);
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }

    // finish() instead of close() since close would close os, which is owned by the caller.
    zipStream.finish();
    LOG.info("Created backup with {} entries", writtenEntryCount.get());
  }

  /**
   * Restores master state from the specified backup.
   *
   * @param is an input stream to read from the backup
   */
  public void initFromBackup(InputStream is) throws IOException {
    try (GzipCompressorInputStream gzIn = new GzipCompressorInputStream(is);
        JournalEntryStreamReader reader = new JournalEntryStreamReader(gzIn)) {
      List<Master> masters = mRegistry.getServers();

      // Executor for reading backup content.
      ExecutorService backupReaderExecutor =
          Executors.newSingleThreadExecutor(ThreadFactoryUtils.build("backup-reader-%d", true));
      // Executor for applying backup.
      ExecutorService backupApplierExecutor =
          Executors.newSingleThreadExecutor(ThreadFactoryUtils.build("backup-applier-%d", true));

      // Entry queue will be used as a buffer and synchronization between readers and appliers.
      LinkedBlockingQueue<JournalEntry> journalEntryQueue = new LinkedBlockingQueue<>(
          ServerConfiguration.getInt(PropertyKey.MASTER_BACKUP_ENTRY_BUFFER_COUNT));

      // Futures for reader/writer tasks.
      SettableFuture<Void> readerTaskFuture = SettableFuture.create();
      SettableFuture<Void> applierTaskFuture = SettableFuture.create();

      // Index masters by name.
      Map<String, Master> mastersByName = Maps.uniqueIndex(masters, Master::getName);

      // Pre-create journal contexts.
      // They should be closed after applying the backup.
      Map<Master, JournalContext> masterJCMap = new HashMap<>();
      for (Master master : masters) {
        masterJCMap.put(master, master.createJournalContext());
      }

      // Shows how many entries have been applied.
      AtomicLong appliedEntryCount = new AtomicLong(0);

      // Create backup reader task.
      backupReaderExecutor.submit(() -> {
        try {
          JournalEntry entry;
          while (!applierTaskFuture.isDone() && (entry = reader.readEntry()) != null) {
            // Try to push the entry as long as applier is alive.
            while (!applierTaskFuture.isDone()) {
              if (journalEntryQueue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
                break;
              }
            }
          }
        } catch (InterruptedException ie) {
          // Store exception.
          readerTaskFuture.setException(ie);
          // Continue interrupt chain.
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while reading from backup stream.", ie);
        } catch (Exception exc) {
          // Store exception.
          readerTaskFuture.setException(exc);
        } finally {
          readerTaskFuture.set(null);
        }
      });
      // Will shutdown as soon as the pending tasks are completed.
      backupReaderExecutor.shutdown();

      // Create applier task.
      backupApplierExecutor.submit(() -> {
        try {
          while (!readerTaskFuture.isDone() || journalEntryQueue.size() > 0) {
            // Drain current elements.
            // Draining entries makes it possible to allow writes while current ones are
            // being applied.
            List<JournalEntry> drainedEntries = new LinkedList<>();
            if (0 == journalEntryQueue.drainTo(drainedEntries)) {
              // No elements at the moment. Fall back to polling.
              JournalEntry entry = journalEntryQueue.poll(10, TimeUnit.MILLISECONDS);
              if (entry == null) {
                // No entry yet.
                continue;
              }
              drainedEntries.add(entry);
            }
            // Apply entries.
            for (JournalEntry entry : drainedEntries) {
              Master master = mastersByName.get(JournalEntryAssociation.getMasterForEntry(entry));
              master.applyAndJournal(masterJCMap.get(master), entry);
            }
          }
        } catch (InterruptedException ie) {
          // Store exception.
          applierTaskFuture.setException(ie);
          // Continue interrupt chain.
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while applying backup content.", ie);
        } catch (Exception exc) {
          // Store exception.
          applierTaskFuture.setException(exc);
        } finally {
          applierTaskFuture.set(null);
        }
      });
      // Will shutdown as soon as the pending tasks are completed.
      backupApplierExecutor.shutdown();

      // Wait until backup initialization is done.
      try {
        // Wait for backup reader.
        readerTaskFuture.get();
        // Wait for backup applier.
        applierTaskFuture.get();
      } catch (InterruptedException ie) {
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while waiting for backup threads.", ie);
      } catch (ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new IOException(cause);
        }
      } finally {
        // Close journal contexts to ensure applied entries are flushed.
        for (JournalContext journalContext : masterJCMap.values()) {
          journalContext.close();
        }
      }

      LOG.info("Restored {} entries from backup", appliedEntryCount.get());
    }
  }
}

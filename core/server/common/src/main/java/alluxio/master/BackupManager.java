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

import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.master.journal.JournalUtils;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.Maps;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Manages creating and restoring from backups.
 *
 * Journals are written as a series of entries in gzip format.
 *
 * TODO(ggezer) Abstract out common logic for backup/initFromBackup methods.
 */
public class BackupManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupManager.class);
  // Used to mark termination of reading/writing journal entries.
  private static final long TERMINATION_SEQ = -1;

  /**
   * The name format of the backup files (E.g., alluxio-backup-2018-12-14-1544749081561.gz).
   * The first value is the current local date of UTC time zone. The second value is
   * the number of milliseconds from the epoch of 1970-01-01T00:00:00Z to the current time.
   */
  public static final String BACKUP_FILE_FORMAT = "alluxio-backup-%s-%s.gz";
  public static final Pattern BACKUP_FILE_PATTERN
      = Pattern.compile("alluxio-backup-[0-9]+-[0-9]+-[0-9]+-([0-9]+).gz");

  private final MasterRegistry mRegistry;

  // Set initial values to -1 to indicate no backup or restore happened
  private long mBackupEntriesCount = -1;
  private long mRestoreEntriesCount = -1;
  private long mBackupTimeMs = -1;
  private long mRestoreTimeMs = -1;

  /**
   * @param registry a master registry containing the masters to backup or restore
   */
  public BackupManager(MasterRegistry registry) {
    mRegistry = registry;
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LAST_BACKUP_ENTRIES_COUNT.getName(),
        () -> mBackupEntriesCount);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LAST_BACKUP_RESTORE_COUNT.getName(),
        () -> mRestoreEntriesCount);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LAST_BACKUP_RESTORE_TIME_MS.getName(),
        () -> mRestoreTimeMs);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LAST_BACKUP_TIME_MS.getName(),
        () -> mBackupTimeMs);
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

    // Executor for taking backup.
    CompletionService<Boolean> completionService = new ExecutorCompletionService<>(
        Executors.newFixedThreadPool(4, ThreadFactoryUtils.build("master-backup-%d", true)));

    // List of active tasks.
    Set<Future<?>> activeTasks = new HashSet<>();

    // Entry queue will be used as a buffer and synchronization between readers and writer.
    // Use of {@link LinkedBlockingQueue} is preferred because of {@code #drainTo()} method,
    // using which all existing entries can be drained while allowing writes.
    // Processing/draining one-by-one using {@link ConcurrentLinkedQueue} proved to be
    // inefficient compared to draining with dedicated method.
    LinkedBlockingQueue<JournalEntry> journalEntryQueue = new LinkedBlockingQueue<>(
        ServerConfiguration.getInt(PropertyKey.MASTER_BACKUP_ENTRY_BUFFER_COUNT));

    // Whether buffering is still active.
    AtomicBoolean bufferingActive = new AtomicBoolean(true);

    // Start the timer for backup metrics.
    long startBackupTime = System.currentTimeMillis();

    // Submit master reader task.
    activeTasks.add(completionService.submit(() -> {
      try {
        for (Master master : mRegistry.getServers()) {
          try (CloseableIterator<JournalEntry> it = master.getJournalEntryIterator()) {
            while (it.get().hasNext()) {
              journalEntryQueue.put(it.get().next());
              if (Thread.interrupted()) {
                throw new InterruptedException();
              }
            }
          }
        }
        // Put termination entry for signaling the writer.
        journalEntryQueue
            .put(JournalEntry.newBuilder().setSequenceNumber(TERMINATION_SEQ).build());
        return true;
      } catch (InterruptedException ie) {
        LOG.info("Backup reader task interrupted");
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while reading master state.", ie);
      } finally {
        // Signal reader completion.
        bufferingActive.set(false);
      }
    }));

    // Submit writer task.
    activeTasks.add(completionService.submit(() -> {
      try {
        List<JournalEntry> pendingEntries = new LinkedList<>();
        while (bufferingActive.get() || journalEntryQueue.size() > 0) {
          // Drain pending entries.
          if (0 == journalEntryQueue.drainTo(pendingEntries)) {
            // No elements at the moment. Fall-back to blocking mode.
            pendingEntries.add(journalEntryQueue.take());
          }
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          // Write entries to back-up stream.
          for (JournalEntry journalEntry : pendingEntries) {
            // Check for termination entry.
            if (journalEntry.getSequenceNumber() == TERMINATION_SEQ) {
              // Reading finished.
              return true;
            }
            journalEntry.writeDelimitedTo(zipStream);
            entryCount.incrementAndGet();
          }
          pendingEntries.clear();
        }
        return true;
      } catch (InterruptedException ie) {
        LOG.info("Backup writer task interrupted");
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while writing to backup stream.", ie);
      }
    }));

    // Wait until backup tasks are completed.
    safeWaitTasks(activeTasks, completionService);

    // Close timer and update entry count.
    mBackupTimeMs = System.currentTimeMillis() - startBackupTime;
    mBackupEntriesCount = entryCount.get();

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
    try (GzipCompressorInputStream gzIn = new GzipCompressorInputStream(is);
        JournalEntryStreamReader reader = new JournalEntryStreamReader(gzIn)) {
      List<Master> masters = mRegistry.getServers();

      // Executor for applying backup.
      CompletionService<Boolean> completionService = new ExecutorCompletionService<>(
          Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("master-backup-%d", true)));

      // List of active tasks.
      Set<Future<?>> activeTasks = new HashSet<>();

      // Entry queue will be used as a buffer and synchronization between readers and appliers.
      LinkedBlockingQueue<JournalEntry> journalEntryQueue = new LinkedBlockingQueue<>(
          ServerConfiguration.getInt(PropertyKey.MASTER_BACKUP_ENTRY_BUFFER_COUNT));

      // Whether still reading from backup.
      AtomicBoolean readingActive = new AtomicBoolean(true);

      // Index masters by name.
      Map<String, Master> mastersByName = Maps.uniqueIndex(masters, Master::getName);

      // Shows how many entries have been applied.
      AtomicLong appliedEntryCount = new AtomicLong(0);

      // Progress executor
      ScheduledExecutorService traceExecutor = Executors.newScheduledThreadPool(1,
          ThreadFactoryUtils.build("master-backup-tracer-%d", true));
      traceExecutor.scheduleAtFixedRate(() -> {
        LOG.info("{} entries from backup applied so far...", appliedEntryCount.get());
      }, 30, 30, TimeUnit.SECONDS);

      // Start the timer for backup metrics.
      long startRestoreTime = System.currentTimeMillis();

      // Create backup reader task.
      activeTasks.add(completionService.submit(() -> {
        try {
          JournalEntry entry;
          while ((entry = reader.readEntry()) != null) {
            journalEntryQueue.put(entry);
          }
          // Put termination entry for signaling the applier.
          journalEntryQueue
              .put(JournalEntry.newBuilder().setSequenceNumber(TERMINATION_SEQ).build());
          return true;
        } catch (InterruptedException ie) {
          // Continue interrupt chain.
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while reading from backup stream.", ie);
        } finally {
          readingActive.set(false);
        }
      }));

      // Create applier task.
      activeTasks.add(completionService.submit(() -> {
        try {
          // Read entries from backup.
          while (readingActive.get() || journalEntryQueue.size() > 0) {
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
            // Apply drained entries.
            // Map for storing journal contexts.
            Map<Master, JournalContext> masterJCMap = new HashMap<>();
            try {
              // Pre-create journal contexts.
              // They should be closed after applying drained entries.
              for (Master master : masters) {
                masterJCMap.put(master, master.createJournalContext());
              }
              // Apply entries.
              for (JournalEntry entry : drainedEntries) {
                // Check for termination entry.
                if (entry.getSequenceNumber() == TERMINATION_SEQ) {
                  // Reading finished.
                  return true;
                }
                String masterName;
                try {
                  masterName = JournalEntryAssociation.getMasterForEntry(entry);
                } catch (IllegalStateException ise) {
                  ProcessUtils.fatalError(LOG, ise, "Unrecognized journal entry: %s", entry);
                  throw ise;
                }
                try {
                  Master master = mastersByName.get(masterName);
                  master.applyAndJournal(masterJCMap.get(master), entry);
                  appliedEntryCount.incrementAndGet();
                } catch (Exception e) {
                  JournalUtils.handleJournalReplayFailure(LOG, e, "Failed to apply "
                          + "journal entry to master %s. Entry: %s", masterName, entry);
                }
              }
            } finally {
              // Close journal contexts to ensure applied entries are flushed,
              // before next round.
              for (JournalContext journalContext : masterJCMap.values()) {
                journalContext.close();
              }
            }
          }
          return true;
        } catch (InterruptedException ie) {
          // Continue interrupt chain.
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while applying backup content.", ie);
        }
      }));

      // Wait until backup tasks are completed and stop metrics timer.
      try {
        safeWaitTasks(activeTasks, completionService);
      } finally {
        mRestoreTimeMs = System.currentTimeMillis() - startRestoreTime;
        mRestoreEntriesCount = appliedEntryCount.get();
        traceExecutor.shutdownNow();
      }

      LOG.info("Restored {} entries from backup", appliedEntryCount.get());
    }
  }

  /**
   * Used to wait until given active tasks are completed or failed.
   *
   * @param activeTasks list of tasks
   * @param completionService completion service used to launch tasks
   * @throws IOException
   */
  private void safeWaitTasks(Set<Future<?>> activeTasks, CompletionService<?> completionService)
      throws IOException {
    // Wait until backup tasks are completed.
    while (activeTasks.size() > 0) {
      try {
        Future<?> resultFuture = completionService.take();
        activeTasks.remove(resultFuture);
        resultFuture.get();
      } catch (InterruptedException ie) {
        // Cancel pending tasks.
        activeTasks.forEach((future) -> future.cancel(true));
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while waiting for backup threads.", ie);
      } catch (ExecutionException ee) {
        // Cancel pending tasks.
        activeTasks.forEach((future) -> future.cancel(true));
        // Throw.
        Throwable cause = ee.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new IOException(cause);
        }
      }
    }
  }
}

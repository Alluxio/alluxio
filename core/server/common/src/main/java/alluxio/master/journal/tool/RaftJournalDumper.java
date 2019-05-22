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

package alluxio.master.journal.tool;

import alluxio.master.journal.JournalEntryAssociation;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.raft.JournalEntryCommand;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.SnapshotReaderStream;
import alluxio.proto.journal.Journal;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of {@link AbstractJournalDumper} for RAFT journals.
 */
public class RaftJournalDumper extends AbstractJournalDumper {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalDumper.class);

  /**
   * Creates embedded journal dumper.
   *
   * @param master journal master
   * @param start journal start sequence
   * @param end journal end sequence
   * @param outputDir output dir for journal dump
   * @param inputDir input dir for journal files
   */
  public RaftJournalDumper(String master, long start, long end, String outputDir, String inputDir)
      throws IOException {
    super(master, start, end, outputDir, inputDir);
  }

  @Override
  void dumpJournal() throws Throwable {
    // Copycat freaks out shown directory is not an actual copycat dir.
    // At least verify that it exists.
    if (!FileUtils.exists(mInputDir)) {
      throw new FileNotFoundException(String.format("Input dir does not exist: %s", mInputDir));
    }
    // Read the journal.
    readFromDir();
  }

  /**
   * Reads from the journal directly instead of going through the raft cluster. The state read this
   * way may be stale, but it can still be useful for debugging while the cluster is offline.
   */
  private void readFromDir() throws Throwable {
    Serializer serializer = RaftJournalSystem.createSerializer();
    serializer.resolve(new ClientRequestTypeResolver());
    serializer.resolve(new ClientResponseTypeResolver());
    serializer.resolve(new ProtocolSerialization());
    serializer.resolve(new ServerSerialization());
    serializer.resolve(new StorageSerialization());

    SingleThreadContext context = new SingleThreadContext("readJournal", serializer);

    try {
      // Read through the whole journal content, starting from snapshot.
      context.execute(this::readCopycatSnapshotFromDir).get();
      context.execute(this::readCopycatLogFromDir).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      context.close();
    }
  }

  private void readCopycatLogFromDir() {
    try (
        PrintStream out =
            new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)));
        Log log = Storage.builder().withDirectory(mInputDir).build().openLog("copycat")) {
      for (long i = log.firstIndex(); i < log.lastIndex(); i++) {
        io.atomix.copycat.server.storage.entry.Entry entry = log.get(i);
        if (entry instanceof CommandEntry) {
          Command command = ((CommandEntry) entry).getCommand();
          if (command instanceof JournalEntryCommand) {
            byte[] entryBytes = ((JournalEntryCommand) command).getSerializedJournalEntry();
            try {
              writeSelected(out, Journal.JournalEntry.parseFrom(entryBytes));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to read logs from journal.", e);
    }
  }

  private void readCopycatSnapshotFromDir() {
    Storage journalStorage = Storage.builder().withDirectory(mInputDir).build();
    if (journalStorage.openSnapshotStore("copycat").snapshots().isEmpty()) {
      LOG.debug("No snapshot found.");
      return;
    }
    Snapshot currentSnapshot = journalStorage.openSnapshotStore("copycat").currentSnapshot();
    SnapshotReader snapshotReader = currentSnapshot.reader();
    String checkpointPath = String.format("%s-%s-%s", mCheckpointsDir, currentSnapshot.index(),
        currentSnapshot.timestamp());

    LOG.debug("Reading snapshot-Id:", snapshotReader.readLong());
    try (CheckpointInputStream checkpointStream =
        new CheckpointInputStream(new SnapshotReaderStream(snapshotReader))) {
      readCheckpoint(checkpointStream, Paths.get(checkpointPath));
    } catch (Exception e) {
      LOG.error("Failed to read snapshot from journal.", e);
    }
  }

  /**
   * Writes given entry after going through range and validity checks.
   *
   * @param out out stream to write the entry to
   * @param entry the entry to write to
   */
  private void writeSelected(PrintStream out, Journal.JournalEntry entry) {
    if (entry == null) {
      return;
    }
    Preconditions.checkState(
        entry.getAllFields().size() <= 1
            || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
        "Raft journal entries should never set multiple fields in addition to sequence "
            + "number, but found %s",
        entry);
    if (entry.getJournalEntriesCount() > 0) {
      // This entry aggregates multiple entries.
      for (Journal.JournalEntry e : entry.getJournalEntriesList()) {
        writeSelected(out, e);
      }
    } else if (entry.toBuilder().clearSequenceNumber().build()
        .equals(Journal.JournalEntry.getDefaultInstance())) {
      // Ignore empty entries, they are created during snapshotting.
    } else {
      if (isSelected(entry)) {
        out.println(entry);
      }
    }
  }

  /**
   * Whether an entry is within the range of provided parameters to the tool.
   *
   * @param entry the journal entry
   * @return {@code true} if the entry should be included in the journal log
   */
  private boolean isSelected(Journal.JournalEntry entry) {
    long sn = entry.getSequenceNumber();
    if (sn >= mStart && sn < mEnd) {
      try {
        return JournalEntryAssociation.getMasterForEntry(entry).equalsIgnoreCase(mMaster);
      } catch (IllegalStateException e) {
        return false;
      }
    }
    return false;
  }
}

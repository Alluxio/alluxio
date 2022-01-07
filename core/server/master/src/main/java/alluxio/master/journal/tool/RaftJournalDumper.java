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
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.proto.journal.Journal;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.List;

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
    // Read through the whole journal content, starting from snapshot.
    readRatisSnapshotFromDir();
    readRatisLogFromDir();
  }

  private void readRatisLogFromDir() {
    try (
        PrintStream out =
            new PrintStream(new BufferedOutputStream(new FileOutputStream(mJournalEntryFile)));
        RaftStorage storage = new RaftStorageImpl(getJournalDir(),
                RaftServerConfigKeys.Log.CorruptionPolicy.getDefault())) {
      List<LogSegmentPath> paths = LogSegmentPath.getLogSegmentPaths(storage);
      for (LogSegmentPath path : paths) {
        final int entryCount = LogSegment.readSegmentFile(path.getPath().toFile(),
                path.getStartEnd(), RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION,
                null, (proto) -> {
              if (proto.hasStateMachineLogEntry()) {
                try {
                  Journal.JournalEntry entry = Journal.JournalEntry.parseFrom(
                      proto.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
                  writeSelected(out, entry);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
        LOG.info("Read {} entries from log {}.", entryCount, path.getPath());
      }
    } catch (Exception e) {
      LOG.error("Failed to read logs from journal.", e);
    }
  }

  private File getJournalDir() {
    return new File(RaftJournalUtils.getRaftJournalDir(new File(mInputDir)),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
  }

  private void readRatisSnapshotFromDir() throws IOException {
    try (RaftStorage storage = new RaftStorageImpl(getJournalDir(),
            RaftServerConfigKeys.Log.CorruptionPolicy.getDefault())) {
      SimpleStateMachineStorage stateMachineStorage = new SimpleStateMachineStorage();
      stateMachineStorage.init(storage);
      SingleFileSnapshotInfo currentSnapshot = stateMachineStorage.getLatestSnapshot();
      if (currentSnapshot == null) {
        LOG.debug("No snapshot found");
        return;
      }
      final File snapshotFile = currentSnapshot.getFile().getPath().toFile();
      String checkpointPath = String.format("%s-%s-%s", mCheckpointsDir, currentSnapshot.getIndex(),
          snapshotFile.lastModified());

      try (DataInputStream inputStream = new DataInputStream(new FileInputStream(snapshotFile))) {
        LOG.debug("Reading snapshot-Id: {}", inputStream.readLong());
        try (CheckpointInputStream checkpointStream = new CheckpointInputStream(inputStream)) {
          readCheckpoint(checkpointStream, Paths.get(checkpointPath));
        } catch (Exception e) {
          LOG.error("Failed to read snapshot from journal.", e);
        }
      } catch (Exception e) {
        LOG.error("Failed to load snapshot {}", snapshotFile, e);
        throw e;
      }
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

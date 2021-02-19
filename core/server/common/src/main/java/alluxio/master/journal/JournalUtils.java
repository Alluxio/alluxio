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

package alluxio.master.journal;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader.Entry;
import alluxio.master.journal.sink.JournalSink;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.util.StreamUtils;

import com.esotericsoftware.kryo.io.OutputChunked;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Utility methods for working with the Alluxio journal.
 */
public final class JournalUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JournalUtils.class);

  /**
   * Returns a URI for the configured location for the specified journal.
   *
   * @return the journal location
   */
  public static URI getJournalLocation() {
    String journalDirectory = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes a checkpoint of the entries in the given iterable.
   *
   * This is the complement of
   * {@link #restoreJournalEntryCheckpoint(CheckpointInputStream, Journaled)}.
   *
   * @param output the stream to write to
   * @param iterable the iterable for fetching journal entries
   */
  public static void writeJournalEntryCheckpoint(OutputStream output, JournalEntryIterable iterable)
      throws IOException, InterruptedException {
    output = new CheckpointOutputStream(output, CheckpointType.JOURNAL_ENTRY);
    try (CloseableIterator<JournalEntry> it = iterable.getJournalEntryIterator()) {
      LOG.info("Write journal entry checkpoint");
      while (it.get().hasNext()) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        it.get().next().writeDelimitedTo(output);
      }
    }
    output.flush();
  }

  /**
   * Restores the given journaled object from the journal entries in the input stream.
   *
   * This is the complement of
   * {@link #writeJournalEntryCheckpoint(OutputStream, JournalEntryIterable)}.
   *
   * @param input the stream to read from
   * @param journaled the object to restore
   */
  public static void restoreJournalEntryCheckpoint(CheckpointInputStream input, Journaled journaled)
      throws IOException {
    Preconditions.checkState(input.getType() == CheckpointType.JOURNAL_ENTRY,
        "Unrecognized checkpoint type when restoring %s: %s", journaled.getCheckpointName(),
        input.getType());
    journaled.resetState();
    LOG.info("Reading journal entries");
    JournalEntryStreamReader reader = new JournalEntryStreamReader(input);
    JournalEntry entry;
    while ((entry = reader.readEntry()) != null) {
      try {
        journaled.processJournalEntry(entry);
      } catch (Throwable t) {
        handleJournalReplayFailure(LOG, t,
            "Failed to process journal entry %s from a journal checkpoint", entry);
      }
    }
  }

  /**
   * Writes a composite checkpoint for the given checkpointed components.
   *
   * This is the complement of {@link #restoreFromCheckpoint(CheckpointInputStream, List)}.
   *
   * @param output the stream to write to
   * @param components the components to checkpoint
   */
  public static void writeToCheckpoint(OutputStream output, List<? extends Checkpointed> components)
      throws IOException, InterruptedException {
    OutputChunked chunked = new OutputChunked(
        new CheckpointOutputStream(output, CheckpointType.COMPOUND), 64 * Constants.KB);
    for (Checkpointed component : components) {
      chunked.writeString(component.getCheckpointName().toString());
      component.writeToCheckpoint(chunked);
      chunked.endChunks();
    }
    chunked.flush();
  }

  /**
   * Restores the given checkpointed components from a composite checkpoint.
   *
   * This is the complement of {@link #writeToCheckpoint(OutputStream, List)}.
   *
   * @param input the stream to read from
   * @param components the components to restore
   */
  public static void restoreFromCheckpoint(CheckpointInputStream input,
      List<? extends Checkpointed> components) throws IOException {
    CompoundCheckpointReader reader = new CompoundCheckpointReader(input);
    Optional<Entry> next;
    while ((next = reader.nextCheckpoint()).isPresent()) {
      Entry nextEntry = next.get();
      boolean found = false;
      for (Checkpointed component : components) {
        if (component.getCheckpointName().equals(nextEntry.getName())) {
          component.restoreFromCheckpoint(nextEntry.getStream());
          found = true;
          break;
        }
      }
      if (!found) {
        throw new RuntimeException(String.format(
            "Unrecognized checkpoint name: %s. Existing components: %s", nextEntry.getName(), Arrays
                .toString(StreamUtils.map(Checkpointed::getCheckpointName, components).toArray())));
      }
    }
  }

  /**
   * Logs a fatal error and exits the system if required.
   *
   * @param logger the logger to log to
   * @param t the throwable causing the fatal error
   * @param format the error message format string
   * @param args args for the format string
   */
  public static void handleJournalReplayFailure(Logger logger, Throwable t,
      String format, Object... args) throws RuntimeException {
    String message = String.format("Journal replay error: " + format, args);
    if (t != null) {
      message += "\n" + Throwables.getStackTraceAsString(t);
    }
    if (ServerConfiguration.getBoolean(PropertyKey.TEST_MODE)) {
      throw new RuntimeException(message);
    }
    logger.error(message);
    if (!ServerConfiguration.getBoolean(PropertyKey.MASTER_JOURNAL_TOLERATE_CORRUPTION)) {
      System.exit(-1);
      throw new RuntimeException(t);
    }
  }

  /**
   * Appends a journal entry to all the supplied journal sinks.
   *
   * @param journalSinks a supplier of journal sinks
   * @param entry the journal entry
   */
  public static void sinkAppend(Supplier<Set<JournalSink>> journalSinks, JournalEntry entry) {
    for (JournalSink sink : journalSinks.get()) {
      sink.append(entry);
    }
  }

  /**
   * Appends a flush to all the supplied journal sinks.
   *
   * @param journalSinks a supplier of journal sinks
   */
  public static void sinkFlush(Supplier<Set<JournalSink>> journalSinks) {
    for (JournalSink sink : journalSinks.get()) {
      sink.flush();
    }
  }

  /**
   * Merge inode entry with subsequent update inode and update inode file entries.
   *
   * @param entries list of journal entries
   * @return a list of compacted journal entries
   */
  public static List<alluxio.proto.journal.Journal.JournalEntry> mergeCreateComplete(
      List<alluxio.proto.journal.Journal.JournalEntry> entries) {
    List<alluxio.proto.journal.Journal.JournalEntry> newEntries = new ArrayList<>();
    // file id : index in the newEntries, InodeFileEntry
    Map<Long, Pair<Integer, File.InodeFileEntry.Builder>> fileEntryMap = new HashMap<>();
    for (alluxio.proto.journal.Journal.JournalEntry oldEntry : entries) {
      if (oldEntry.hasInodeFile()) {
        // Use the old entry as a placeholder, to be replaced later
        newEntries.add(oldEntry);
        fileEntryMap.put(oldEntry.getInodeFile().getId(),
            new Pair<>(newEntries.size() - 1,
                File.InodeFileEntry.newBuilder(oldEntry.getInodeFile())));
      } else if (oldEntry.hasUpdateInode()) {
        File.UpdateInodeEntry entry = oldEntry.getUpdateInode();
        if (fileEntryMap.get(entry.getId()) == null) {
          newEntries.add(oldEntry);
          continue;
        }
        File.InodeFileEntry.Builder builder = fileEntryMap.get(entry.getId()).getSecond();
        if (entry.hasAcl()) {
          builder.setAcl(entry.getAcl());
        }
        if (entry.hasCreationTimeMs()) {
          builder.setCreationTimeMs(entry.getCreationTimeMs());
        }
        if (entry.hasGroup() && !entry.getGroup().isEmpty()) {
          builder.setGroup(entry.getGroup());
        }
        if (entry.hasLastModificationTimeMs() && (entry.getOverwriteModificationTime()
            || entry.getLastModificationTimeMs() > builder.getLastModificationTimeMs())) {
          builder.setLastModificationTimeMs(entry.getLastModificationTimeMs());
        }
        if (entry.hasLastAccessTimeMs() && (entry.getOverwriteAccessTime()
            || entry.getLastAccessTimeMs() > builder.getLastAccessTimeMs())) {
          builder.setLastAccessTimeMs(entry.getLastAccessTimeMs());
        }
        if (entry.hasMode()) {
          builder.setMode((short) entry.getMode());
        }
        if (entry.getMediumTypeCount() != 0) {
          builder.clearMediumType();
          builder.addAllMediumType(new HashSet<>(entry.getMediumTypeList()));
        }
        if (entry.hasName()) {
          builder.setName(entry.getName());
        }
        if (entry.hasOwner() && !entry.getOwner().isEmpty()) {
          builder.setOwner(entry.getOwner());
        }
        if (entry.hasParentId()) {
          builder.setParentId(entry.getParentId());
        }
        if (entry.hasPersistenceState()) {
          builder.setPersistenceState(entry.getPersistenceState());
        }
        if (entry.hasPinned()) {
          builder.setPinned(entry.getPinned());
        }
        if (entry.hasTtl()) {
          builder.setTtl(entry.getTtl());
        }
        if (entry.hasTtlAction()) {
          builder.setTtlAction(entry.getTtlAction());
        }
        if (entry.hasUfsFingerprint()) {
          builder.setUfsFingerprint(entry.getUfsFingerprint());
        }
        if (entry.getXAttrCount() > 0) {
          builder.clearXAttr();
          builder.putAllXAttr(new HashMap<>(entry.getXAttrMap()));
        }
      } else if (oldEntry.hasUpdateInodeFile()) {
        File.UpdateInodeFileEntry entry = oldEntry.getUpdateInodeFile();
        if (fileEntryMap.get(entry.getId()) == null) {
          newEntries.add(oldEntry);
          continue;
        }
        File.InodeFileEntry.Builder builder = fileEntryMap.get(entry.getId()).getSecond();
        if (entry.hasPersistJobId()) {
          builder.setPersistJobId(entry.getPersistJobId());
        }
        if (entry.hasReplicationMax()) {
          builder.setReplicationMax(entry.getReplicationMax());
        }
        if (entry.hasReplicationMin()) {
          builder.setReplicationMin(entry.getReplicationMin());
        }
        if (entry.hasTempUfsPath()) {
          builder.setTempUfsPath(entry.getTempUfsPath());
        }
        if (entry.hasBlockSizeBytes()) {
          builder.setBlockSizeBytes(entry.getBlockSizeBytes());
        }
        if (entry.hasCacheable()) {
          builder.setCacheable(entry.getCacheable());
        }
        if (entry.hasCompleted()) {
          builder.setCompleted(entry.getCompleted());
        }
        if (entry.hasLength()) {
          builder.setLength(entry.getLength());
        }
        if (entry.getSetBlocksCount() > 0) {
          builder.clearBlocks();
          builder.addAllBlocks(entry.getSetBlocksList());
        }
      } else {
        newEntries.add(oldEntry);
      }
    }
    for (Pair<Integer, File.InodeFileEntry.Builder> pair : fileEntryMap.values()) {
      // Replace the old entry place holder with the new entry,
      // to create the file in the same place in the journal
      newEntries.set(pair.getFirst(),
          Journal.JournalEntry.newBuilder().setInodeFile(pair.getSecond()).build());
    }
    return newEntries;
  }

  private JournalUtils() {} // prevent instantiation
}

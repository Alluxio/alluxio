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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader;
import alluxio.master.journal.checkpoint.CompoundCheckpointFormat.CompoundCheckpointReader.Entry;
import alluxio.master.journal.sink.JournalSink;
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
import java.util.Arrays;
import java.util.List;
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

  private JournalUtils() {} // prevent instantiation
}

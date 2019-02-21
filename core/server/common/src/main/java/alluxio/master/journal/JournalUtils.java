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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.StreamUtils;

import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.OutputChunked;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Utility methods for working with the Alluxio journal.
 */
public final class JournalUtils {

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
   * This is the complement of {@link #restoreJournalEntryCheckpoint(InputStream, Journaled)}.
   *
   * @param output the stream to write to
   * @param iterable the iterable for fetching journal entries
   */
  public static void writeJournalEntryCheckpoint(OutputStream output, JournalEntryIterable iterable)
      throws IOException, InterruptedException {
    Iterator<JournalEntry> it = iterable.getJournalEntryIterator();
    while (it.hasNext()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      it.next().writeDelimitedTo(output);
    }
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
  public static void restoreJournalEntryCheckpoint(InputStream input, Journaled journaled)
      throws IOException {
    journaled.resetState();
    try (JournalEntryStreamReader reader = new JournalEntryStreamReader(input)) {
      JournalEntry entry;
      while ((entry = reader.readEntry()) != null) {
        journaled.processJournalEntry(entry);
      }
    }
  }

  /**
   * Writes a composite checkpoint for the given checkpointed components.
   *
   * This is the complement of {@link #restoreFromCheckpoint(InputStream, List)}.
   *
   * @param output the stream to write to
   * @param components the components to checkpoint
   */
  public static void writeToCheckpoint(OutputStream output, List<? extends Checkpointed> components)
      throws IOException, InterruptedException {
    try (OutputChunked chunked = new OutputChunked(output)) {
      for (Checkpointed component : components) {
        chunked.writeString(component.getName());
        component.writeToCheckpoint(chunked);
        chunked.endChunks();
      }
    }
  }

  /**
   * Restores the given checkpointed components from a composite checkpoint.
   *
   * This is the complement of {@link #writeToCheckpoint(OutputStream, List)}.
   *
   * @param input the stream to read from
   * @param components the components to restore
   */
  public static void restoreFromCheckpoint(InputStream input,
      List<? extends Checkpointed> components) throws IOException {
    try (InputChunked chunked = new InputChunked(input)) {
      String name = chunked.readString();
      for (Checkpointed component : components) {
        if (component.getName().equals(name)) {
          component.restoreFromCheckpoint(chunked);
          chunked.nextChunks();
          continue;
        }
      }
      throw new RuntimeException(
          String.format("Unrecognized component name: %s. Existing components: %s", name,
              Arrays.toString(StreamUtils.map(Checkpointed::getName, components).toArray())));
    }
  }

  private JournalUtils() {} // prevent instantiation
}

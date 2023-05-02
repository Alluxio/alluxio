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

package alluxio.master.journal.checkpoint;

import alluxio.master.journal.JournalEntryStreamReader;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

/**
 * Format for checkpoints written as sequential JournalEntry protocol buffers.
 */
public class JournalCheckpointFormat implements CheckpointFormat {
  private static final String ENTRY_SEPARATOR = Strings.repeat("-", 80);

  @Override
  public JournalCheckpointReader createReader(CheckpointInputStream in) {
    return new JournalCheckpointReader(in);
  }

  @Override
  public void parseToHumanReadable(CheckpointInputStream in, PrintStream out) throws IOException {
    JournalCheckpointReader reader = createReader(in);
    Optional<JournalEntry> entry;
    while ((entry = reader.nextEntry()).isPresent()) {
      out.println(ENTRY_SEPARATOR);
      out.println(entry.get());
    }
  }

  /**
   * Reads journal entries from a checkpoint stream. The underlying stream will not be closed.
   */
  public static class JournalCheckpointReader implements CheckpointReader {
    private final JournalEntryStreamReader mReader;

    /**
     * @param in a checkpoint stream to read from
     */
    public JournalCheckpointReader(CheckpointInputStream in) {
      Preconditions.checkState(in.getType() == CheckpointType.JOURNAL_ENTRY,
          "Unexpected checkpoint type: " + in.getType());
      mReader = new JournalEntryStreamReader(in);
    }

    /**
     * @return the next journal entry, or empty if we've reached the end
     */
    public Optional<JournalEntry> nextEntry() throws IOException {
      return Optional.ofNullable(mReader.readEntry());
    }
  }
}

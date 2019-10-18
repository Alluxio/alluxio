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

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.StreamUtils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Convenience interface for classes which delegate journaling to a list of journaled components.
 */
public interface DelegatingGroupJournaled extends Journaled {
  /**
   * @return the journaled components to delegate journaling to
   */
  List<Journaled> getDelegates();

  @Override
  default boolean processJournalEntry(JournalEntry entry) {
    for (Journaled journaled : getDelegates()) {
      if (journaled.processJournalEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  @Override
  default void resetState() {
    // we resetState in the reverse order that we replay the journal
    Lists.reverse(getDelegates()).forEach(Journaled::resetState);
  }

  @Override
  default void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    JournalUtils.writeToCheckpoint(output, getDelegates());
  }

  @Override
  default void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    JournalUtils.restoreFromCheckpoint(input, getDelegates());
  }

  @Override
  default Iterator<JournalEntry> getJournalEntryIterator() {
    List<Iterator<JournalEntry>> componentIters = StreamUtils
        .map(JournalEntryIterable::getJournalEntryIterator, getDelegates());
    return Iterators.concat(componentIters.iterator());
  }
}

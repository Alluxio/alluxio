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
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableIterator;
import alluxio.util.StreamUtils;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Convenience class which groups together multiple Journaled components as a single Journaled
 * component.
 */
public class JournaledGroup implements Journaled {
  private final List<Journaled> mJournaled;
  private final CheckpointName mCheckpointName;

  /**
   * Creates an instance of a single journaled component, from a list of journaled components.
   *
   * @param journaled the list of journaled components to be grouped
   * @param checkpointName the name of the journal checkpoint for the group
   */
  public JournaledGroup(List<Journaled> journaled, CheckpointName checkpointName) {
    mJournaled = journaled;
    mCheckpointName = checkpointName;
  }

  /**
   * @return the list of journaled components in this group
   */
  public List<Journaled> getJournaled() {
    return mJournaled;
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    for (Journaled journaled : mJournaled) {
      if (journaled.processJournalEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void resetState() {
    // resetState in the reverse order that we replay the journal
    Lists.reverse(mJournaled).forEach(Journaled::resetState);
  }

  @Override
  public CheckpointName getCheckpointName() {
    return mCheckpointName;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    JournalUtils.writeToCheckpoint(output, mJournaled);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    JournalUtils.restoreFromCheckpoint(input, mJournaled);
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    List<CloseableIterator<JournalEntry>> componentIters = StreamUtils
        .map(JournalEntryIterable::getJournalEntryIterator, mJournaled);
    return CloseableIterator.concat(componentIters);
  }
}

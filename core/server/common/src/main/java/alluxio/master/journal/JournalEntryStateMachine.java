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

import alluxio.proto.journal.Journal.JournalEntry;

import java.io.IOException;

/**
 * Interface for a state machine which operates on {@link JournalEntry}s.
 */
public interface JournalEntryStateMachine extends JournalEntryIterable {
  /**
   * @return the name of this journal entry state machine
   */
  String getName();

  /**
   * Applies a journal entry to the state machine.
   *
   * @param entry the entry to process to update the state of the state machine
   */
  void processJournalEntry(JournalEntry entry) throws IOException;

  /**
   * Resets the journaled internal state of the state machine.
   */
  void resetState();
}

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

package alluxio.master.journal.sink;

import alluxio.proto.journal.Journal.JournalEntry;

/**
 * The interface for the journal to push events.  Implementations must complete these actions
 * quickly, since this is executed in the critical sections of journal writing.
 */
public interface JournalSink {
  /**
   * Appends a new journal entry written to the journal.
   *
   * @param entry the entry to append
   */
  default void append(JournalEntry entry) {}

  /**
   * Signals the sink that the journal is flushed.
   */
  default void flush() {}

  /**
   * Signals the sink that the journal is about to shutdown.
   */
  default void beforeShutdown() {}
}

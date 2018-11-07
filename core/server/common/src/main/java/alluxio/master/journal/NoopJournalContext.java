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

import javax.annotation.concurrent.ThreadSafe;

/**
 * Noop version of JournalContext.
 */
@ThreadSafe
public final class NoopJournalContext implements JournalContext {
  public static final NoopJournalContext INSTANCE = new NoopJournalContext();

  /**
   * Constructs the {@link NoopJournalContext}.
   */
  private NoopJournalContext() {
    // Do nothing
  }

  @Override
  public void append(JournalEntry entry) {
    // Do nothing
  }

  @Override
  public void close() {
    // Do nothing
  }
}

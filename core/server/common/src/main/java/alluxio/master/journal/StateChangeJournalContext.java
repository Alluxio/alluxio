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

import alluxio.exception.status.UnavailableException;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for correctly managing the state change lock for a wrapped journal context.
 */
@NotThreadSafe
public final class StateChangeJournalContext implements JournalContext {
  private static final Logger LOG = LoggerFactory.getLogger(StateChangeJournalContext.class);
  private final JournalContext mJournalContext;
  private final LockResource mStateLockResource;

  /**
   * Constructs a {@link StateChangeJournalContext}.
   *
   * @param journalContext the journal context to wrap
   * @param stateLockResource the state lock resource to keep
   */
  public StateChangeJournalContext(JournalContext journalContext, LockResource stateLockResource) {
    Preconditions.checkNotNull(journalContext, "journalContext");
    mJournalContext = journalContext;
    mStateLockResource = stateLockResource;
  }

  @Override
  public void append(JournalEntry entry) {
    mJournalContext.append(entry);
  }

  @Override
  public void close() throws UnavailableException {
    try {
      mJournalContext.close();
    } finally {
      // must release the state lock after the journal context is closed.
      mStateLockResource.close();
    }
  }
}

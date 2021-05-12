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

package alluxio.master.journal.raft;

import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.MasterJournalContext;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class associated with each master that lets the master create journal contexts for writing
 * entries to the journal.
 */
@NotThreadSafe
public class RaftJournal implements Journal {
  private final Journaled mStateMachine;
  private final AtomicReference<AsyncJournalWriter> mJournalWriter;
  private final URI mLocation;

  /**
   * @param stateMachine the state machine for this journal
   * @param location the location of the raft journal
   * @param journalWriter reference to the journal writer to use for writing to the journal
   */
  public RaftJournal(Journaled stateMachine, URI location,
      AtomicReference<AsyncJournalWriter> journalWriter) {
    mStateMachine = stateMachine;
    mJournalWriter = journalWriter;
    mLocation = location;
  }

  /**
   * @return the state machine for this journal
   */
  public Journaled getStateMachine() {
    return mStateMachine;
  }

  @Override
  public URI getLocation() {
    return mLocation;
  }

  @Override
  public JournalContext createJournalContext() throws UnavailableException {
    AsyncJournalWriter journalWriter = mJournalWriter.get();
    if (journalWriter == null) {
      throw new UnavailableException("Journal has been closed");
    }
    return new MasterJournalContext(journalWriter);
  }

  @Override
  public void close() throws IOException {
    // Nothing to close.
  }
}

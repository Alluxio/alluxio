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

package alluxio.master;

import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.executor.ExecutorServiceFactory;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.time.Clock;
import java.util.Iterator;

/**
 * A base class for masters which don't journal any state.
 */
public abstract class AbstractNonJournaledMaster extends AbstractMaster {
  /**
   * @param masterContext the context for Alluxio master
   * @param clock the Clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for
   */
  protected AbstractNonJournaledMaster(MasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, clock, executorServiceFactory);
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // No journal.
  }

  @Override
  public void resetState() {
    // No journal.
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.emptyIterator();
  }
}

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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for merging journal entries together for a wrapped journal context.
 */
@NotThreadSafe
public final class MergeJournalContext implements JournalContext {
  private static final Logger LOG = LoggerFactory.getLogger(MergeJournalContext.class);
  private final JournalContext mJournalContext;
  private final UnaryOperator<List<JournalEntry>> mMergeOperator;
  private final List<JournalEntry> mJournalEntries;

  /**
   * Constructs a {@link MergeJournalContext}.
   *
   * @param journalContext the journal context to wrap
   */
  public MergeJournalContext(JournalContext journalContext, UnaryOperator<List<JournalEntry>> merger) {
    Preconditions.checkNotNull(journalContext, "journalContext");
    mJournalContext = journalContext;
    mMergeOperator = merger;
    mJournalEntries = new ArrayList<>();
  }

  @Override
  public void append(JournalEntry entry) {
    mJournalEntries.add(entry);
  }

  @Override
  public void close() throws UnavailableException {
    List<JournalEntry> mergedEntries = mMergeOperator.apply(mJournalEntries);
    mergedEntries.forEach(mJournalContext::append);
    // Note that we do not close the enclosing journal context here
  }
}

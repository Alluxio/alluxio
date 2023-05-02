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

package alluxio.master.journal.ufs;

import alluxio.master.journal.AbstractJournalProgressLogger;

import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Journal replay logger for the {@link UfsJournal}.
 */
public class UfsJournalProgressLogger extends AbstractJournalProgressLogger {

  private final UfsJournal mJournal;
  private final Supplier<Long> mCommitSupplier;

  /**
   * Creates a new instance of the journal replay progress logger.
   *
   * @param journal the journal being replayed
   * @param endCommitIdx the final commit index (sequence number) in the journal
   * @param lastIndexSupplier supplier that gives the last applied commit (sequence number)
   */
  public UfsJournalProgressLogger(UfsJournal journal, OptionalLong endCommitIdx,
      Supplier<Long> lastIndexSupplier) {
    super(endCommitIdx);
    mJournal = journal;
    mCommitSupplier = lastIndexSupplier;
  }

  @Override
  public long getLastAppliedIndex() {
    return mCommitSupplier.get();
  }

  @Override
  public String getJournalName() {
    return mJournal.toString();
  }
}

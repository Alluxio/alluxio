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

import alluxio.master.journal.AbstractJournalProgressLogger;

import java.util.OptionalLong;

/**
 * Logs journal replay progress for the {@link RaftJournalSystem}.
 */
public class RaftJournalProgressLogger extends AbstractJournalProgressLogger {

  private final JournalStateMachine mJournal;

  /**
   * Creates a new instance of the logger.
   *
   * @param journal the journal being replayed
   * @param endCommitIdx the final commit index in the log to estimate completion times
   */
  public RaftJournalProgressLogger(JournalStateMachine journal, OptionalLong endCommitIdx) {
    super(endCommitIdx);
    mJournal = journal;
  }

  @Override
  public long getLastAppliedIndex() {
    return mJournal.getLastAppliedCommitIndex();
  }

  @Override
  public String getJournalName() {
    return "embedded journal";
  }
}

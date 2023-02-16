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

package alluxio.stress.cli.journaldisruptor;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalType;
import alluxio.proto.journal.Journal;

/**
 * JournalReader, an extra(redundant) wrap of EntryStream.
 *
 */

public class JournalReader {
  private EntryStream mStream;

  /**
   * Init JournalReader that corresponding to the journal type.
   * @param mMaster
   * @param mStart
   * @param mEnd
   * @param mInputDir
   */
  public JournalReader(String mMaster, long mStart, long mEnd, String mInputDir) {
    JournalType journalType = Configuration
        .getEnum(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.class);
    switch (journalType) {
      case UFS:
        mStream = new UfsJournalEntryStream(mMaster, mStart, mEnd, mInputDir);
        break;
      case EMBEDDED:
        mStream = new RaftJournalEntryStream(mMaster, mStart, mEnd, mInputDir);
        break;
      default:
        System.err.printf("Unsupported journal type: %s%n", journalType.name());
        return;
    }
  }

  /**
   * the nextEntry method, call the EntryStream.nextEntry().
   * @return next Alluxio journal entry
   */
  public Journal.JournalEntry nextEntry() {
    return mStream.nextEntry();
  }
}

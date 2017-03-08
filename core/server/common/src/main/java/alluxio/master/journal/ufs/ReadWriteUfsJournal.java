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

import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.ReadWriteJournal;

import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The read-write journal. This allows both reads and writes to the journal.
 */
@ThreadSafe
public class ReadWriteUfsJournal extends ReadOnlyUfsJournal implements ReadWriteJournal {
  /**
   * @param location the location for the journal
   */
  public ReadWriteUfsJournal(URL location) {
    super(location);
  }

  /**
   * @return the {@link UfsJournalWriter} for this journal
   */
  public JournalWriter getNewWriter() {
    return new UfsJournalWriter(this);
  }
}

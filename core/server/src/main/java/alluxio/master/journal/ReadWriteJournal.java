/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The read-write journal. This allows both reads and writes to the journal.
 */
@ThreadSafe
public class ReadWriteJournal extends ReadOnlyJournal {
  /**
   * @param directory the base directory for the journal
   */
  public ReadWriteJournal(String directory) {
    super(directory);
  }

  /**
   * @return the {@link JournalWriter} for this journal
   */
  public JournalWriter getNewWriter() {
    return new JournalWriter(this);
  }
}

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
import alluxio.master.journal.MutableJournal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link MutableJournal} based on UFS.
 */
@ThreadSafe
public class UfsMutableJournal extends UfsJournal implements MutableJournal {
  /**
   * @param location the location for the journal
   */
  public UfsMutableJournal(URI location) {
    super(location);
  }

  @Override
  public JournalWriter getWriter() {
    return new UfsJournalWriter(this);
  }
}

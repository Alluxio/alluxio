/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.journal;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

public class JournalReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  private final TachyonConf mTachyonConf;

  JournalReader(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
  }

  /**
   * Checks to see if the journal checkpoint has not be updated. If it has been updated since the
   * creation of this reader, this reader is no longer valid.
   *
   * @return true if the checkpoint point has not been modified.
   */
  public boolean isValid() {
    // TODO
    return false;
  }

  public JournalEntry readCheckpoint() {
    // TODO
    return null;
  }

  public List<JournalEntry> readEntries() {
    // TODO
    return Collections.emptyList();
  }
}

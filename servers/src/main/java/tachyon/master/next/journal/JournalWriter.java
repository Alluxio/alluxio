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

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

import java.io.IOException;

public class JournalWriter {
  private final Journal mJournal;
  private final TachyonConf mTachyonConf;
  private final UnderFileSystem mUfs;

  JournalWriter(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
    mUfs = UnderFileSystem.get(mJournal.getDirectory(), mTachyonConf);
  }

  public void writeCheckpoint(JournalEntry entry) {
  }

  public void writeEntry(JournalEntry entry) {
    // TODO
  }

  public void flush() {
    // TODO
  }

  public void close() throws IOException {
    mUfs.close();
  }
}

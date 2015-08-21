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

import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

public class JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Journal mJournal;
  private final TachyonConf mTachyonConf;
  private final String mJournalDirectory;
  private final String mCheckpointPath;
  private final UnderFileSystem mUfs;

  JournalWriter(Journal journal, TachyonConf tachyonConf) {
    mJournal = journal;
    mTachyonConf = tachyonConf;
    mJournalDirectory = mJournal.getDirectory();
    mCheckpointPath = mJournalDirectory + mJournal.getCheckpointFilename();
    mUfs = UnderFileSystem.get(mJournalDirectory, mTachyonConf);
  }

  public void writeCheckpoint(JournalEntry entry) throws IOException {
    String tmpCheckpointPath = mCheckpointPath + ".tmp";
    LOG.info("Creating tmp checkpoint file: " + tmpCheckpointPath);
    if (!mUfs.exists(mJournalDirectory)) {
      LOG.info("Creating journal folder: " + mJournalDirectory);
      mUfs.mkdirs(mJournalDirectory, true);
    }
    DataOutputStream dos = new DataOutputStream(mUfs.create(tmpCheckpointPath));
    mJournal.getJournalFormatter().serialize(entry, dos);
    dos.flush();
    dos.close();

    LOG.info("Successfully created tmp checkpoint file: " + tmpCheckpointPath);
    mUfs.delete(mCheckpointPath, false);
    mUfs.rename(tmpCheckpointPath, mCheckpointPath);
    mUfs.delete(tmpCheckpointPath, false);
    LOG.info("Renamed checkpoint file " + tmpCheckpointPath + " to " + mCheckpointPath);
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

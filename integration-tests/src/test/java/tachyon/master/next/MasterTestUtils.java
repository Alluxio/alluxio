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

package tachyon.master.next;

import java.io.IOException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.next.block.BlockMaster;
import tachyon.master.next.filesystem.FileSystemMaster;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalReader;

public class MasterTestUtils {
  public static FileSystemMaster createFileSystemMasterFromJournal(TachyonConf tachyonConf)
      throws IOException {
    String masterJournal = tachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);

    Journal blockJournal = new Journal(BlockMaster.getJournalDirectory(masterJournal),
        tachyonConf);
    BlockMaster blockMaster = new BlockMaster(blockJournal, tachyonConf);

    Journal fsJournal = new Journal(FileSystemMaster.getJournalDirectory(masterJournal),
        tachyonConf);
    FileSystemMaster fsMaster = new FileSystemMaster(tachyonConf, blockMaster, fsJournal);

    JournalReader reader = fsJournal.getNewReader();
    // load checkpoint
    fsMaster.processJournalCheckpoint(reader.getCheckpointInputStream());

    // load logs
    JournalInputStream is;
    while ((is = reader.getNextInputStream()) != null) {
      JournalEntry entry;
      while ((entry = is.getNextEntry()) != null) {
        fsMaster.processJournalEntry(entry);
      }
      is.close();
    }

    return fsMaster;
  }
}

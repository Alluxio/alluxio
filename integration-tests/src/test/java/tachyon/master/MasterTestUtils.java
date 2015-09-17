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

package tachyon.master;

import java.io.IOException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;

public class MasterTestUtils {
  public static FileSystemMaster createFileSystemMasterFromJournal(TachyonConf tachyonConf)
      throws IOException {
    String masterJournal = tachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
    Journal blockJournal = new ReadWriteJournal(BlockMaster.getJournalDirectory(masterJournal));
    Journal fsJournal = new ReadWriteJournal(FileSystemMaster.getJournalDirectory(masterJournal));
    BlockMaster blockMaster = new BlockMaster(blockJournal);
    FileSystemMaster fsMaster = new FileSystemMaster(blockMaster, fsJournal);
    blockMaster.start(true);
    fsMaster.start(true);
    return fsMaster;
  }
}

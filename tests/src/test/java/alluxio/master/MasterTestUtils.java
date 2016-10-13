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

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadOnlyJournal;
import alluxio.master.journal.ReadWriteJournal;

import java.io.IOException;

public class MasterTestUtils {
  public static FileSystemMaster createLeaderFileSystemMasterFromJournal()
      throws IOException {
    String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    Journal blockJournal = new ReadWriteJournal(BlockMaster.getJournalDirectory(masterJournal));
    Journal fsJournal = new ReadWriteJournal(FileSystemMaster.getJournalDirectory(masterJournal));
    BlockMaster blockMaster = new BlockMaster(blockJournal);
    FileSystemMaster fsMaster = new FileSystemMaster(blockMaster, fsJournal);
    blockMaster.start(true);
    fsMaster.start(true);
    return fsMaster;
  }

  public static FileSystemMaster createStandbyFileSystemMasterFromJournal()
      throws IOException {
    String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    Journal blockJournal = new ReadOnlyJournal(BlockMaster.getJournalDirectory(masterJournal));
    Journal fsJournal = new ReadOnlyJournal(FileSystemMaster.getJournalDirectory(masterJournal));
    BlockMaster blockMaster = new BlockMaster(blockJournal);
    FileSystemMaster fsMaster = new FileSystemMaster(blockMaster, fsJournal);
    blockMaster.start(false);
    fsMaster.start(false);
    return fsMaster;
  }
}

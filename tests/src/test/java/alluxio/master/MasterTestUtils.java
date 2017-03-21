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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;

import java.net.URI;

public class MasterTestUtils {

  /**
   * Creates a new {@link FileSystemMaster} from journal.
   *
   * @return a new {@link FileSystemMaster}
   */
  public static FileSystemMaster createLeaderFileSystemMasterFromJournal() throws Exception {
    String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    JournalFactory factory = new MutableJournal.Factory(new URI(masterJournal));
    BlockMaster blockMaster = new BlockMaster(factory);
    FileSystemMaster fsMaster = new FileSystemMaster(blockMaster, factory);
    blockMaster.start(true);
    fsMaster.start(true);
    return fsMaster;
  }

  /**
   * Creates a new standby {@link FileSystemMaster} from journal.
   *
   * @return a new {@link FileSystemMaster}
   */
  public static FileSystemMaster createStandbyFileSystemMasterFromJournal() throws Exception {
    String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    JournalFactory factory = new MutableJournal.Factory(new URI(masterJournal));
    BlockMaster blockMaster = new BlockMaster(factory);
    FileSystemMaster fsMaster = new FileSystemMaster(blockMaster, factory);
    blockMaster.start(false);
    fsMaster.start(false);
    return fsMaster;
  }

  /**
   * Waits for the startup consistency check to complete with a limit of 1 minute.
   *
   * @param master the file system master which is starting up
   */
  public static void waitForStartupConsistencyCheck(final FileSystemMaster master) {
    CommonUtils.waitFor("Startup consistency check completion", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void aVoid) {
        return master.getStartupConsistencyCheck().getStatus()
            == FileSystemMaster.StartupConsistencyCheck.Status.COMPLETE;
      }
    }, WaitForOptions.defaults().setTimeout(Constants.MINUTE_MS));
  }
}

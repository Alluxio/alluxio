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
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterFactory;
import alluxio.master.file.StartupConsistencyCheck.Status;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;

import java.net.URI;

public class MasterTestUtils {

  /**
   * Creates a new leader {@link FileSystemMaster} from journal along with its dependencies, and
   * returns the master registry containing that master.
   *
   * @return a master registry containing the created {@link FileSystemMaster} master
   */
  public static MasterRegistry createLeaderFileSystemMasterFromJournal() throws Exception {
    return createFileSystemMasterFromJournal(true);
  }

  /**
   * Creates a new standby {@link FileSystemMaster} from journal along with its dependencies, and
   * returns the master registry containing that master.
   *
   * @return a master registry containing the created {@link FileSystemMaster} master
   */
  public static MasterRegistry createStandbyFileSystemMasterFromJournal() throws Exception {
    return createFileSystemMasterFromJournal(false);
  }

  /**
   * Creates a new {@link FileSystemMaster} from journal along with its dependencies, and returns
   * the master registry containing that master.
   *
   * @param isLeader whether to start as a leader
   * @return a master registry containing the created {@link FileSystemMaster} master
   */
  private static MasterRegistry createFileSystemMasterFromJournal(boolean isLeader)
      throws Exception {
    String masterJournal = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    MasterRegistry registry = new MasterRegistry();
    JournalFactory factory = new Journal.Factory(new URI(masterJournal));
    new BlockMasterFactory().create(registry, factory);
    new FileSystemMasterFactory().create(registry, factory);
    registry.start(isLeader);
    return registry;
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
        return master.getStartupConsistencyCheck().getStatus() != Status.RUNNING;
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }
}

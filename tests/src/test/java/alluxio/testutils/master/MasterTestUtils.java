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

package alluxio.testutils.master;

import static org.mockito.Mockito.mock;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.BackupManager;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.TestSafeModeManager;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterFactory;
import alluxio.master.file.StartupConsistencyCheck.Status;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import java.util.concurrent.TimeoutException;

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
    SafeModeManager safeModeManager = new TestSafeModeManager();
    long startTimeMs = System.currentTimeMillis();
    int port = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
    JournalSystem journalSystem = JournalTestUtils.createJournalSystem(masterJournal);
    CoreMasterContext masterContext = new CoreMasterContext(journalSystem, safeModeManager,
        mock(BackupManager.class), startTimeMs, port);
    new MetricsMasterFactory().create(registry, masterContext);
    new BlockMasterFactory().create(registry, masterContext);
    new FileSystemMasterFactory().create(registry, masterContext);
    journalSystem.start();
    if (isLeader) {
      journalSystem.gainPrimacy();
    }
    registry.start(isLeader);
    return registry;
  }

  /**
   * Waits for the startup consistency check to complete with a limit of 1 minute.
   *
   * @param master the file system master which is starting up
   */
  public static void waitForStartupConsistencyCheck(final FileSystemMaster master)
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("Startup consistency check completion",
        () -> master.getStartupConsistencyCheck().getStatus() != Status.RUNNING,
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }
}

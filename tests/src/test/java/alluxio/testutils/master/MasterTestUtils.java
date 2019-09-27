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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.BackupManager;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterUtils;
import alluxio.master.SafeModeManager;
import alluxio.master.TestSafeModeManager;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterFactory;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.security.user.ServerUserState;
import alluxio.security.user.UserState;
import alluxio.underfs.MasterUfsManager;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class MasterTestUtils {

  /**
   * Creates a new leader {@link FileSystemMaster} from journal along with its dependencies, and
   * returns the master registry and the journal system.
   *
   * @return a resource that contains the master registry and the journal system
   */
  public static FsMasterResource createLeaderFileSystemMasterFromJournal() throws Exception {
    return createFileSystemMasterFromJournal(true, null);
  }

  /**
   * Creates a new leader {@link FileSystemMaster} from journal along with its dependencies, and
   * returns the master registry and the journal system.
   *
   * @param userState the user state for the server
   * @return a resource that contains the master registry and the journal system
   */
  public static FsMasterResource createLeaderFileSystemMasterFromJournal(UserState userState)
      throws Exception {
    return createFileSystemMasterFromJournal(true, userState);
  }

  /**
   * Creates a new standby {@link FileSystemMaster} from journal along with its dependencies, and
   * returns the master registry and the journal system.
   *
   * @return a resource that contains the master registry and the journal system
   */
  public static FsMasterResource createStandbyFileSystemMasterFromJournal() throws Exception {
    return createFileSystemMasterFromJournal(false, null);
  }

  /**
   * Creates a new leader {@link FileSystemMaster} from a copy of the journal along with its
   * dependencies, and returns the master registry and the journal system.
   *
   * @return a resource that contains the master registry and the journal system
   */
  public static FsMasterResource createLeaderFileSystemMasterFromJournalCopy() throws Exception {
    String masterJournal = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    File tmpDirFile = Files.createTempDir();
    tmpDirFile.deleteOnExit();
    String tempDir = tmpDirFile.getAbsolutePath();
    FileUtils.copyDirectory(new File(masterJournal), new File(tempDir));
    return createFileSystemMasterFromJournal(true, null, tempDir);
  }

  /**
   * Creates a new {@link FileSystemMaster} from journal along with its dependencies, and returns
   * the master registry and the journal system.
   *
   * @param isLeader whether to start as a leader
   * @param userState the user state for the server. if null, will use ServerUserState.global()
   * @return a resource that contains the master registry and the journal system
   */
  private static FsMasterResource createFileSystemMasterFromJournal(boolean isLeader,
      UserState userState) throws Exception {
    String masterJournal = ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    return createFileSystemMasterFromJournal(isLeader, userState, masterJournal);
  }

  /**
   * Creates a new {@link FileSystemMaster} from journal along with its dependencies, and returns
   * the master registry and the journal system.
   *
   * @param isLeader whether to start as a leader
   * @param userState the user state for the server. if null, will use ServerUserState.global()
   * @param journalFolder the folder of the master journal
   * @return a resource that contains the master registry and the journal system
   */
  private static FsMasterResource createFileSystemMasterFromJournal(boolean isLeader,
      UserState userState, String journalFolder) throws Exception {
    String masterJournal = journalFolder;
    MasterRegistry registry = new MasterRegistry();
    SafeModeManager safeModeManager = new TestSafeModeManager();
    long startTimeMs = System.currentTimeMillis();
    int port = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_PORT);
    String baseDir = ServerConfiguration.get(PropertyKey.MASTER_METASTORE_DIR);
    JournalSystem journalSystem = JournalTestUtils.createJournalSystem(masterJournal);
    if (userState == null) {
      userState = ServerUserState.global();
    }
    CoreMasterContext masterContext = CoreMasterContext.newBuilder()
        .setJournalSystem(journalSystem)
        .setSafeModeManager(safeModeManager)
        .setBackupManager(mock(BackupManager.class))
        .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
        .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
        .setStartTimeMs(startTimeMs)
        .setUserState(userState)
        .setPort(port)
        .setUfsManager(new MasterUfsManager())
        .build();
    new MetricsMasterFactory().create(registry, masterContext);
    new BlockMasterFactory().create(registry, masterContext);
    new FileSystemMasterFactory().create(registry, masterContext);
    journalSystem.start();
    if (isLeader) {
      journalSystem.gainPrimacy();
    }
    registry.start(isLeader);
    return new FsMasterResource(registry, journalSystem);
  }
}

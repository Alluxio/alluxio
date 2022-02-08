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

import static org.mockito.Mockito.mock;

import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.heap.HeapBlockStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.security.user.UserState;
import alluxio.underfs.MasterUfsManager;

/**
 * Util methods to help with master testing.
 */
public final class MasterTestUtils {

  /**
   * @return a basic master context for the purpose of testing
   */
  public static CoreMasterContext testMasterContext() {
    return testMasterContext(new NoopJournalSystem());
  }

  /**
   * @return a basic master context for the purpose of testing
   * @param journalSystem a journal system to use in the context
   */
  public static CoreMasterContext testMasterContext(JournalSystem journalSystem) {
    return testMasterContext(journalSystem, null);
  }

  /**
   * @return a basic master context for the purpose of testing
   * @param journalSystem a journal system to use in the context
   * @param userState the user state to use in the context
   */
  public static CoreMasterContext testMasterContext(JournalSystem journalSystem,
      UserState userState) {
    return testMasterContext(journalSystem, userState,
        HeapBlockStore::new, x -> new HeapInodeStore());
  }

  /**
   * @return a basic master context for the purpose of testing
   * @param journalSystem a journal system to use in the context
   * @param userState the user state to use in the context
   * @param blockStoreFactory a factory to create {@link BlockStore}
   * @param inodeStoreFactory a factory to create {@link InodeStore}
   */
  public static CoreMasterContext testMasterContext(
      JournalSystem journalSystem, UserState userState,
      BlockStore.Factory blockStoreFactory,
      InodeStore.Factory inodeStoreFactory) {
    return CoreMasterContext.newBuilder()
        .setJournalSystem(journalSystem)
        .setUserState(userState)
        .setSafeModeManager(new TestSafeModeManager())
        .setBackupManager(mock(BackupManager.class))
        .setBlockStoreFactory(blockStoreFactory)
        .setInodeStoreFactory(inodeStoreFactory)
        .setStartTimeMs(-1)
        .setPort(-1)
        .setUfsManager(new MasterUfsManager())
        .build();
  }

  private MasterTestUtils() {} // Not intended for instantiation.
}

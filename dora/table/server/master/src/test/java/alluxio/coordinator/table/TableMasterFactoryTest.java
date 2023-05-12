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

package alluxio.coordinator.table;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import alluxio.Constants;
import alluxio.Server;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.coordinator.AlwaysStandbyPrimarySelector;
import alluxio.coordinator.BackupManager;
import alluxio.coordinator.CoreMasterContext;
import alluxio.coordinator.MasterRegistry;
import alluxio.coordinator.MasterUtils;
import alluxio.coordinator.TestSafeModeManager;
import alluxio.coordinator.journal.noop.NoopJournalSystem;
import alluxio.coordinator.metastore.heap.HeapBlockMetaStore;
import alluxio.coordinator.metastore.heap.HeapInodeStore;
import alluxio.underfs.MasterUfsManager;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Set;
import java.util.stream.Collectors;

public class TableMasterFactoryTest {

  private CoreMasterContext mContext;

  @ClassRule
  public static TemporaryFolder sTemp = new TemporaryFolder();

  @Before
  public void before() {
    mContext = CoreMasterContext.newBuilder()
        .setJournalSystem(new NoopJournalSystem())
        .setPrimarySelector(new AlwaysStandbyPrimarySelector())
        .setSafeModeManager(new TestSafeModeManager())
        .setBackupManager(mock(BackupManager.class))
        .setBlockStoreFactory(HeapBlockMetaStore::new)
        .setInodeStoreFactory(x -> new HeapInodeStore())
        .setUfsManager(new MasterUfsManager())
        .build();
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, sTemp.getRoot().getAbsolutePath());
  }

  @After
  public void after() {
    Configuration.set(PropertyKey.TABLE_ENABLED, true);
  }

  @Test
  public void enabled() throws Exception {
    Configuration.set(PropertyKey.TABLE_ENABLED, true);
    MasterRegistry registry = new MasterRegistry();
    MasterUtils.createMasters(registry, mContext);
    Set<String> names =
        registry.getServers().stream().map(Server::getName).collect(Collectors.toSet());
    assertTrue(names.contains(Constants.TABLE_MASTER_NAME));
  }

  @Test
  public void disabled() {
    Configuration.set(PropertyKey.TABLE_ENABLED, false);
    MasterRegistry registry = new MasterRegistry();
    MasterUtils.createMasters(registry, mContext);
    Set<String> names =
        registry.getServers().stream().map(Server::getName).collect(Collectors.toSet());
    assertFalse(names.contains(Constants.TABLE_MASTER_NAME));
  }
}

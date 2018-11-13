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
import alluxio.master.metastore.rocks.RocksMetastore;

import org.rocksdb.RocksDBException;

/**
 * Util methods to help with master testing.
 */
public final class MasterTestUtils {

  /**
   * @return a basic master context for the purpose of testing
   */
  public static MasterContext testMasterContext() {
    return testMasterContext(new NoopJournalSystem());
  }

  /**
   * @return a basic master context for the purpose of testing
   * @param journalSystem a journal system to use in the context
   */
  public static MasterContext testMasterContext(JournalSystem journalSystem) {
    try {
      return new MasterContext(journalSystem, new TestSafeModeManager(),
          mock(BackupManager.class), new RocksMetastore(), -1, -1);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private MasterTestUtils() {} // Not intended for instatiation.
}

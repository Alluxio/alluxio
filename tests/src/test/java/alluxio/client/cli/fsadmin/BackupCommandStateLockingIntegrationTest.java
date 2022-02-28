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

package alluxio.client.cli.fsadmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.master.MasterContext;
import alluxio.master.MasterProcess;
import alluxio.master.StateLockManager;
import alluxio.master.StateLockOptions;
import alluxio.resource.LockResource;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Integration tests for the backup command's interaction with the state-locking.
 */
@LocalAlluxioClusterResource.ServerConfig(confParams = {
    PropertyKey.Name.MASTER_BACKUP_DIRECTORY, "${alluxio.work.dir}/backups",
    PropertyKey.Name.MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION, "3s",
    PropertyKey.Name.MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT, "3s"})
public final class BackupCommandStateLockingIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void timeoutWhenStateLockAcquired() throws Exception {
    // Grab the master state-lock-manager via reflection.
    MasterProcess masterProcess =
        Whitebox.getInternalState(mLocalAlluxioCluster.getLocalAlluxioMaster(), "mMasterProcess");
    MasterContext masterCtx = Whitebox.getInternalState(masterProcess, "mContext");
    StateLockManager stateLockManager = masterCtx.getStateLockManager();

    // Lock the state-change lock on the master before initiating the backup.
    try (LockResource lr = stateLockManager.lockExclusive(StateLockOptions.defaults())) {
      // Prepare for a backup.
      Path dir = Paths.get(ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY));
      Files.createDirectories(dir);
      assertEquals(0, Files.list(dir).count());
      // Initiate backup. It should fail.
      int errCode = mFsAdminShell.run("backup");
      assertTrue(mOutput.toString().contains(ExceptionMessage.STATE_LOCK_TIMED_OUT
          .getMessage(3000/* matching the cluster resource configuration. */)));
      assertNotEquals(0, errCode);
    }
  }

  @Test
  public void takeBackupDuringExclusiveOnlyPhase() throws Exception {
    // Grab the master state-lock-manager via reflection.
    MasterProcess masterProcess =
        Whitebox.getInternalState(mLocalAlluxioCluster.getLocalAlluxioMaster(), "mMasterProcess");
    MasterContext masterCtx = Whitebox.getInternalState(masterProcess, "mContext");
    StateLockManager stateLockManager = masterCtx.getStateLockManager();

    // Activate exclusive-only phase via reflection.
    // Cannot make via cluster property due to lack of support from test utilities.
    long exclusiveOnlyDurationMs = 30000;
    Whitebox.setInternalState(stateLockManager, "mExclusiveOnlyDeadlineMs",
        System.currentTimeMillis() + exclusiveOnlyDurationMs);

    try {
      // getStatus() should fail during exclusive-only phase.
      mException.expect(AlluxioException.class);
      mLocalAlluxioCluster.getClient().getStatus(new AlluxioURI("/"));
      // Prepare for a backup.
      Path dir = Paths.get(ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY));
      Files.createDirectories(dir);
      assertEquals(0, Files.list(dir).count());
      // Take the backup. It should be allowed.
      int errCode = mFsAdminShell.run("backup");
      assertEquals("", mErrOutput.toString());
      assertEquals(0, errCode);
      assertEquals(1, Files.list(dir).count());
    } finally {
      // Reset exclusive-only phase.
      Whitebox.setInternalState(stateLockManager, "mExclusiveOnlyDeadlineMs", -1);
    }
  }
}

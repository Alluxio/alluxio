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

import alluxio.AlluxioTestDirectory;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.Name;
import alluxio.conf.ServerConfiguration;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Integration tests for the backup command.
 */
@LocalAlluxioClusterResource.ServerConfig(
    confParams = {
        Name.MASTER_BACKUP_DIRECTORY, "${alluxio.work.dir}/backups",
        Name.MASTER_SHELL_BACKUP_STATE_LOCK_TRY_DURATION, "3s",
        Name.MASTER_SHELL_BACKUP_STATE_LOCK_TIMEOUT, "3s"})
public final class BackupCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void defaultDirectory() throws IOException {
    Path dir = Paths.get(ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY));
    Files.createDirectories(dir);
    assertEquals(0, Files.list(dir).count());
    int errCode = mFsAdminShell.run("backup");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    assertEquals(2, Files.list(dir).count());
  }

  @Test
  public void specificDirectory() throws IOException {
    Path dir = AlluxioTestDirectory.createTemporaryDirectory("test-backup").toPath();
    Files.createDirectories(dir);
    assertEquals(0, Files.list(dir).count());
    int errCode = mFsAdminShell.run("backup", dir.toAbsolutePath().toString());
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    assertEquals(2, Files.list(dir).count());
  }
}

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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.io.Files;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests the pin command with multiple media.
 */
public final class PinCommandMultipleMediaIntegrationTest extends BaseIntegrationTest {
  private static final int SIZE_BYTES = Constants.MB * 16;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          // These are in common with PinCommandIntegrationTest, which sets properties in
          // AbstractShellIntegrationTest.
          .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, "false")
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH")
          // multiple media
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS
              .format(1), Constants.MEDIUM_SSD)
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
              Files.createTempDir().getAbsolutePath())
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
              Files.createTempDir().getAbsolutePath())
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0),
              String.valueOf(SIZE_BYTES))
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
              String.valueOf(SIZE_BYTES))
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE
                  .format(0), Constants.MEDIUM_SSD)
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE
                  .format(1), Constants.MEDIUM_SSD)
          .build();

  @Rule
  public TestRule mResetRule = sLocalAlluxioClusterResource.getResetResource();

  @Test
  public void setPinToSpecificMedia() throws Exception {
    FileSystem fileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemShell fsShell = new FileSystemShell(ServerConfiguration.global());

    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI filePathB = new AlluxioURI("/testFileB");

    int fileSize = SIZE_BYTES / 2;

    FileSystemTestUtils.createByteFile(fileSystem, filePathA, WritePType.MUST_CACHE,
        fileSize);
    assertTrue(fileSystem.exists(filePathA));

    assertEquals(0, fsShell.run("pin", filePathA.toString(), Constants.MEDIUM_SSD));
    int ret = fsShell.run("setReplication", "-min", "2", filePathA.toString());
    assertEquals(0, ret);

    assertEquals(Constants.MEDIUM_SSD, fileSystem.getStatus(filePathA).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().get(0).getMediumType());

    assertEquals(-1, fsShell.run("pin", filePathB.toString(), "NVRAM"));
  }
}

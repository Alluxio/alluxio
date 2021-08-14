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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.io.Files;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.IOException;

/**
 * Tests the pin command with multiple media.
 */
public final class PinCommandMultipleMediaIntegrationTest extends BaseIntegrationTest {
  private static final int SIZE_BYTES = Constants.MB * 16;

  private static LocalAlluxioJobCluster sJobCluster;

  private static WaitForOptions sWaitOptions
      = WaitForOptions.defaults().setTimeoutMs(60 * Constants.SECOND_MS).setInterval(1000);

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
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, SIZE_BYTES / 2)
          // multiple media
          .setProperty(PropertyKey.MASTER_REPLICATION_CHECK_INTERVAL_MS, "500ms")
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS
              .format(1), Constants.MEDIUM_SSD)
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
              Files.createTempDir().getAbsolutePath())
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
              Files.createTempDir().getAbsolutePath()
                  + "," + Files.createTempDir().getAbsolutePath())
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(0),
              String.valueOf(SIZE_BYTES))
          .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
              SIZE_BYTES + "," + SIZE_BYTES)
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE
                  .format(0), Constants.MEDIUM_MEM)
          .setProperty(
              PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_MEDIUMTYPE
                  .format(1), Constants.MEDIUM_SSD + "," + Constants.MEDIUM_SSD)
          .build();

  @Rule
  public TestRule mResetRule = sLocalAlluxioClusterResource.getResetResource();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sJobCluster = new LocalAlluxioJobCluster();
    sJobCluster.start();
  }

  @Before
  public void beforeTest() throws Exception {
    sJobCluster.stop();
    sJobCluster.start();
  }

  @Test
  public void setPinToSpecificMedia() throws Exception {
    FileSystem fileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemShell fsShell = new FileSystemShell(ServerConfiguration.global());

    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI filePathB = new AlluxioURI("/testFileB");

    int fileSize = SIZE_BYTES / 2;

    FileSystemTestUtils.createByteFile(fileSystem, filePathA, WritePType.CACHE_THROUGH,
        fileSize);
    assertTrue(fileSystem.exists(filePathA));

    assertEquals(0, fsShell.run("pin", filePathA.toString(), Constants.MEDIUM_SSD));
    CommonUtils
        .waitFor("File being moved", () -> sJobCluster.getMaster().getJobMaster().listDetailed()
            .stream().anyMatch(x -> x.getName().equals("Move")
                    && x.getStatus().equals(Status.COMPLETED)
                    && x.getAffectedPaths().contains(filePathA.getPath())),
            sWaitOptions);

    assertTrue(fileSystem.getStatus(filePathA).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().stream()
        .anyMatch(x -> x.getMediumType().equals(Constants.MEDIUM_SSD)));

    assertEquals(-1, fsShell.run("pin", filePathB.toString(), "NVRAM"));
  }

  private static boolean fileExists(FileSystem fs, AlluxioURI path) {
    try {
      return fs.exists(path);
    } catch (IOException e) {
      return false;
    } catch (AlluxioException e) {
      return false;
    }
  }

  @Test
  public void pinToMediumForceEviction() throws Exception {
    FileSystem fileSystem = sLocalAlluxioClusterResource.get().getClient();
    FileSystemShell fsShell = new FileSystemShell(ServerConfiguration.global());

    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI dirPath = new AlluxioURI("/testDirA");
    AlluxioURI filePathB = new AlluxioURI(dirPath.getPath() + "/testFileB");
    AlluxioURI filePathC = new AlluxioURI("/testFileC");
    int fileSize = SIZE_BYTES / 2;

    FileSystemTestUtils.createByteFile(fileSystem, filePathA, WritePType.CACHE_THROUGH,
        fileSize);
    assertTrue(fileExists(fileSystem, filePathA));
    assertEquals(0, fsShell.run("pin", filePathA.toString(), "MEM"));

    URIStatus status = fileSystem.getStatus(filePathA);
    assertTrue(status.isPinned());
    assertTrue(status.getPinnedMediumTypes().contains("MEM"));

    fileSystem.createDirectory(dirPath);
    assertEquals(0, fsShell.run("pin", dirPath.toString(), "MEM"));

    FileSystemTestUtils.createByteFile(fileSystem, filePathB, WritePType.CACHE_THROUGH,
        fileSize);
    assertTrue(fileExists(fileSystem, filePathB));

    URIStatus statusB = fileSystem.getStatus(filePathB);
    assertTrue(statusB.isPinned());
    assertTrue(statusB.getPinnedMediumTypes().contains("MEM"));
    FileSystemTestUtils.createByteFile(fileSystem, filePathC, WritePType.THROUGH,
        fileSize);

    assertEquals(100, fileSystem.getStatus(filePathA).getInAlluxioPercentage());
    assertEquals(100, fileSystem.getStatus(filePathB).getInAlluxioPercentage());
    assertEquals(0, fileSystem.getStatus(filePathC).getInAlluxioPercentage());

    assertEquals(0, fsShell.run("pin", filePathC.toString(), "SSD"));

    // Verify files are replicated into the correct tier through job service
    CommonUtils
        .waitFor("File being loaded", () -> sJobCluster.getMaster().getJobMaster().listDetailed()
                .stream()
                .anyMatch(x -> x.getStatus().equals(Status.COMPLETED)
                    && x.getName().equals("Replicate")
                    && x.getAffectedPaths().contains(filePathC.getPath())),
            sWaitOptions);

    assertEquals(100, fileSystem.getStatus(filePathC).getInAlluxioPercentage());

    assertEquals(Constants.MEDIUM_MEM, fileSystem.getStatus(filePathA).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().get(0).getMediumType());
    assertEquals(Constants.MEDIUM_MEM, fileSystem.getStatus(filePathB).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().get(0).getMediumType());

    assertEquals(Constants.MEDIUM_SSD, fileSystem.getStatus(filePathC).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().get(0).getMediumType());

    assertEquals(0, fsShell.run("unpin", filePathA.toString()));

    assertEquals(0, fsShell.run("pin", filePathC.toString(), "MEM"));

    status = fileSystem.getStatus(filePathA);
    assertFalse(status.isPinned());
    assertTrue(status.getPinnedMediumTypes().isEmpty());

    // Verify files are migrated from another tier into the correct tier through job service
    // Also verify that eviction works
    CommonUtils
        .waitFor("File being moved", () -> sJobCluster.getMaster().getJobMaster().listDetailed()
                .stream()
                .anyMatch(x -> x.getStatus().equals(Status.COMPLETED)
                    && x.getName().equals("Move")
                    && x.getAffectedPaths().contains(filePathC.getPath())),
            sWaitOptions);
    assertEquals(0, fileSystem.getStatus(filePathA).getInAlluxioPercentage());

    assertEquals(Constants.MEDIUM_MEM, fileSystem.getStatus(filePathC).getFileBlockInfos()
        .get(0).getBlockInfo().getLocations().get(0).getMediumType());
  }
}

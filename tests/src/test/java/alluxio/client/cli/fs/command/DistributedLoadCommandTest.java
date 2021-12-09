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

import alluxio.AlluxioURI;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.fs.command.DistributedLoadCommand;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for {@link DistributedLoadCommand}.
 */
public final class DistributedLoadCommandTest extends AbstractFileSystemShellTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @ClassRule
  public static LocalAlluxioClusterResource sResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(4)
          .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH")
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, SIZE_BYTES / 2)
          .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
          .build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalAlluxioCluster = sResource.get();
    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.start();
    sFileSystem = sLocalAlluxioCluster.getClient();
    sJobMaster = sLocalAlluxioJobCluster.getMaster().getJobMaster();
    sJobShell = new alluxio.cli.job.JobShell(ServerConfiguration.global());
    sFsShell = new FileSystemShell(ServerConfiguration.global());
  }

  @Test
  public void loadDir() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileA", WritePType.THROUGH,
        10);
    FileSystemTestUtils
        .createByteFile(fs, "/testRoot/testFileB", WritePType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = fs.getStatus(uriA);
    URIStatus statusB = fs.getStatus(uriB);
    Assert.assertNotEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testRoot");
    statusA = fs.getStatus(uriA);
    statusB = fs.getStatus(uriB);
    Assert.assertEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
  }

  @Test
  public void loadFile() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testFileNew", WritePType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFileNew");
    URIStatus status = fs.getStatus(uri);
    Assert.assertNotEquals(100, status.getInMemoryPercentage());
    // Testing loading of a single file
    sFsShell.run("distributedLoad", "/testFileNew");
    status = fs.getStatus(uri);
    Assert.assertEquals(100, status.getInMemoryPercentage());
  }

  @Test
  public void loadFileMultiCopy() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testFile", WritePType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = fs.getStatus(uri);
    Assert.assertNotEquals(100, status.getInMemoryPercentage());
    // Testing loading of a single file
    sFsShell.run("distributedLoad", "/testFile", "--replication", "1");
    status = fs.getStatus(uri);
    Assert.assertEquals(100, status.getInMemoryPercentage());
    Assert.assertEquals(1, status.getFileBlockInfos().get(0).getBlockInfo().getLocations().size());
    sFsShell.run("distributedLoad", "/testFile", "--replication", "3");
    status = fs.getStatus(uri);
    Assert.assertEquals(3, status.getFileBlockInfos().get(0).getBlockInfo().getLocations().size());
  }

  @Test
  public void loadIndexFile() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testFileA", WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(fs, "/testFileB", WritePType.THROUGH, 10);

    File testFile = mTempFolder.newFile("testFile");
    FileUtils.writeStringToFile(testFile, "/testFileA\n/testFileB\n", StandardCharsets.UTF_8);
    AlluxioURI uriA = new AlluxioURI("/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testFileB");
    URIStatus statusA = fs.getStatus(uriA);
    URIStatus statusB = fs.getStatus(uriB);
    Assert.assertNotEquals(100, statusA.getInMemoryPercentage());
    Assert.assertNotEquals(100, statusB.getInMemoryPercentage());
    sFsShell.run("distributedLoad", "--index", testFile.getAbsolutePath());
    statusA = fs.getStatus(uriA);
    statusB = fs.getStatus(uriB);
    Assert.assertEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
  }

  @Test
  public void loadIndexFileInBatch() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testBatchFileA", WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(fs, "/testBatchFileB", WritePType.THROUGH, 10);
    File testFile = mTempFolder.newFile("testFile");
    FileUtils.writeStringToFile(testFile, "/testBatchFileA\n/testBatchFileB\n",
        StandardCharsets.UTF_8);
    AlluxioURI uriA = new AlluxioURI("/testBatchFileA");
    AlluxioURI uriB = new AlluxioURI("/testBatchFileB");
    URIStatus statusA = fs.getStatus(uriA);
    URIStatus statusB = fs.getStatus(uriB);
    Assert.assertNotEquals(100, statusA.getInMemoryPercentage());
    Assert.assertNotEquals(100, statusB.getInMemoryPercentage());
    sFsShell.run("distributedLoad", "--index", testFile.getAbsolutePath(), "--batch-size", "2");
    statusA = fs.getStatus(uriA);
    statusB = fs.getStatus(uriB);
    Assert.assertEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
  }

  @Test
  public void loadDirInBatch() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testBatchRoot/testBatchFileA", WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(fs, "/testBatchRoot/testBatchFileB", WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(fs, "/testBatchRoot/testBatchFileC", WritePType.THROUGH, 10);
    AlluxioURI uriA = new AlluxioURI("/testBatchRoot/testBatchFileA");
    AlluxioURI uriB = new AlluxioURI("/testBatchRoot/testBatchFileB");
    AlluxioURI uriC = new AlluxioURI("/testBatchRoot/testBatchFileC");
    URIStatus statusA = fs.getStatus(uriA);
    URIStatus statusB = fs.getStatus(uriB);
    URIStatus statusC = fs.getStatus(uriC);
    Assert.assertNotEquals(100, statusA.getInMemoryPercentage());
    Assert.assertNotEquals(100, statusB.getInMemoryPercentage());
    Assert.assertNotEquals(100, statusC.getInMemoryPercentage());
    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testBatchRoot", "--batch-size", "2");
    statusA = fs.getStatus(uriA);
    statusB = fs.getStatus(uriB);
    statusC = fs.getStatus(uriC);
    Assert.assertEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
    Assert.assertEquals(100, statusC.getInMemoryPercentage());
  }

  @Test
  public void loadDirWithLotsFilesInBatch() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    int fileSize = 100;
    List<AlluxioURI> uris = new ArrayList<>(fileSize);
    for (int i = 0; i < fileSize; i++) {
      FileSystemTestUtils.createByteFile(fs, "/testBatchRoot/testBatchFile" + i, WritePType.THROUGH,
          10);

      AlluxioURI uri = new AlluxioURI("/testBatchRoot/testBatchFile" + i);
      uris.add(uri);
      URIStatus status = fs.getStatus(uri);
      Assert.assertNotEquals(100, status.getInMemoryPercentage());
    }

    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testBatchRoot", "--batch-size", "3");
    for (AlluxioURI uri : uris) {
      URIStatus status = fs.getStatus(uri);
      Assert.assertEquals(100, status.getInMemoryPercentage());
    }
  }
}

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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.apache.commons.collections.CollectionUtils;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Long.MAX_VALUE)
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
    sJobShell = new alluxio.cli.job.JobShell(Configuration.global());
    sFsShell = new FileSystemShell(Configuration.global());
  }

  @Test
  public void loadDir() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileLoadDirA", WritePType.THROUGH,
        10);
    FileSystemTestUtils
        .createByteFile(fs, "/testRoot/testFileLoadDirB", WritePType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileLoadDirA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileLoadDirB");

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

  @Test
  public void loadDirWithPassiveCache() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileD", WritePType.THROUGH,
        10);
    AlluxioURI uriD = new AlluxioURI("/testRoot/testFileD");

    URIStatus status = fs.getStatus(uriD);
    Assert.assertNotEquals(100, status.getInMemoryPercentage());
    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testRoot", "--passive-cache");
    status = fs.getStatus(uriD);
    Assert.assertEquals(100, status.getInMemoryPercentage());
  }

  @Test
  public void loadDirWithPassiveCacheInBatch() throws IOException, AlluxioException {
    FileSystem fs = sResource.get().getClient();
    int fileSize = 20;
    List<AlluxioURI> uris = new ArrayList<>(fileSize);
    for (int i = 0; i < fileSize; i++) {
      FileSystemTestUtils.createByteFile(fs, "/testBatchRoot/testBatchFilePassive" + i,
          WritePType.THROUGH, 10);

      AlluxioURI uri = new AlluxioURI("/testBatchRoot/testBatchFilePassive" + i);
      uris.add(uri);
      URIStatus status = fs.getStatus(uri);
      Assert.assertNotEquals(100, status.getInMemoryPercentage());
    }
    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testBatchRoot", "--batch-size", "3", "--passive-cache");
    for (AlluxioURI uri : uris) {
      URIStatus status = fs.getStatus(uri);
      Assert.assertEquals(100, status.getInMemoryPercentage());
    }
  }

  @Test
  public void loadDirWithCorrectCount() throws IOException, AlluxioException {
    FileSystemShell fsShell = new FileSystemShell(Configuration.global());
    FileSystem fs = sResource.get().getClient();
    int fileSize = 66;
    for (int i = 0; i < fileSize; i++) {
      FileSystemTestUtils.createByteFile(fs, "/testCount/testBatchFile" + i, WritePType.THROUGH,
          10);
      AlluxioURI uri = new AlluxioURI("/testCount/testBatchFile" + i);
      URIStatus status = fs.getStatus(uri);
      Assert.assertNotEquals(100, status.getInMemoryPercentage());
    }
    fsShell.run("distributedLoad", "/testCount", "--batch-size", "3");
    String[] output = mOutput.toString().split("\n");
    Assert.assertEquals(String.format(
            "Total completed file count is %s, failed file count is 0", fileSize),
        output[output.length - 2]);
  }

  @Test
  public void loadDirWithFailure() throws IOException, AlluxioException {
    FileSystemShell fsShell = new FileSystemShell(Configuration.global());
    FileSystem fs = sResource.get().getClient();
    int fileSize = 20;
    List<String> failures = new ArrayList<>();
    for (int i = 0; i < fileSize; i++) {
      String pathStr = "/testFailure/testBatchFile" + i;
      FileSystemTestUtils.createByteFile(fs, pathStr, WritePType.THROUGH, 10);
      if (i % 2 == 0) {
        AlluxioURI uri = new AlluxioURI(pathStr);
        URIStatus fileInfo = fs.getStatus(uri);
        String path = fileInfo.getFileInfo().getUfsPath();
        boolean result = new File(path).delete();
        Assert.assertTrue(result);
        failures.add(pathStr);
      }
    }
    String failureFilePath = "./logs/user/distributedLoad_testFailure_failures.csv";

    fsShell.run("distributedLoad", "/testFailure");
    Set<String> failedPathsFromJobMasterLog = sJobMaster.getAllFailedPaths();
    System.out.println(failedPathsFromJobMasterLog);

    Assert.assertEquals(failedPathsFromJobMasterLog.size(), fileSize / 2);
    Assert.assertTrue(CollectionUtils.isEqualCollection(failures, failedPathsFromJobMasterLog));

    Assert.assertTrue(mOutput.toString().contains(
        String.format("Total completed file count is %s,"
              + " failed file count is %s\n", fileSize / 2, fileSize / 2)));
    Assert.assertTrue(mOutput.toString()
        .contains(String.format("Check out %s for"
               + " full list of failed files.", failureFilePath)));
    List<String> failuresFromFile = Files.readAllLines(Paths.get(failureFilePath));
    Assert.assertTrue(CollectionUtils.isEqualCollection(failures, failuresFromFile));
  }

  @Test
  public void testAsyncForLoad() throws IOException {
    FileSystem fs = sResource.get().getClient();
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileAsyncA", WritePType.THROUGH,
            10);
    FileSystemTestUtils
            .createByteFile(fs, "/testRoot/testFileAsyncB", WritePType.MUST_CACHE, 10);

    sFsShell.run("distributedLoad", "--async", "/testRoot");

    String[] output = mOutput.toString().split("\n");
    Assert.assertTrue(Arrays.toString(output).contains("Entering async submission mode"));
    Assert.assertTrue(Arrays.toString(output).contains("Submitted distLoad job successfully"));
  }
}

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
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LoadCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();
  @ClassRule
  public static LocalAlluxioClusterResource sResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(1)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "16MB")
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, 1)
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_HDD)
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_HDD)
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "2GB")
          .build();

  @Test
  public void testCommand() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.THROUGH,
        Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileB", WritePType.THROUGH,
        Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDirectory/testFileC",
        WritePType.THROUGH, Constants.MB);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");
    AlluxioURI uriC = new AlluxioURI("/testRoot/testDirectory/testFileC");
    assertEquals(0, sFileSystem.getStatus(uriA).getInAlluxioPercentage());
    assertEquals(0, sFileSystem.getStatus(uriB).getInAlluxioPercentage());
    assertEquals(0, sFileSystem.getStatus(uriC).getInAlluxioPercentage());
    // Testing loading of a directory
    sFsShell.run("loadMetadata", "/testRoot");
    assertEquals(0, sFsShell.run("load", "/testRoot", "--submit", "--verify"));
    assertEquals(0, sFsShell.run("load", "/testRoot", "--progress"));
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriA, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriB, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriC, 100);
    while (!mOutput.toString().contains("SUCCEEDED")) {
      assertEquals(0, sFsShell.run("load", "/testRoot", "--progress"));
      Thread.sleep(1000);
    }
    assertTrue(mOutput.toString().contains("Files Processed: 3 out of 3"));
    assertTrue(mOutput.toString().contains("Bytes Loaded: 3072.00KB out of 3072.00KB"));
    assertTrue(mOutput.toString().contains("Files Failed: 0"));
    assertEquals(0, sFsShell.run("load", "/testRoot", "--stop"));
    assertEquals(-2, sFsShell.run("load", "/testRootNotExists", "--progress"));
    assertTrue(mOutput.toString().contains("Load for path '/testRootNotExists' cannot be found."));
    sFsShell.run("load", "/testRoot", "--progress", "--format", "JSON");
    assertTrue(mOutput.toString().contains("\"mJobState\":\"SUCCEEDED\""));
    sFsShell.run("load", "/testRoot", "--progress", "--format", "JSON", "--verbose");
    assertTrue(mOutput.toString().contains("\"mVerbose\":true"));
  }

  @Test
  public void testPartialListing() throws Exception {
    int numFiles = 500;
    for (int i = 0; i < numFiles; i++) {
      String fileName = "/testRoot/testFile" + i;
      String fileName2 = "/testRoot/testDirectory/testFile" + i;
      FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.THROUGH, Constants.MB);
      FileSystemTestUtils.createByteFile(sFileSystem, fileName2, WritePType.THROUGH, Constants.MB);
      assertEquals(0, sFileSystem.getStatus(new AlluxioURI(fileName)).getInAlluxioPercentage());
      assertEquals(0, sFileSystem.getStatus(new AlluxioURI(fileName2)).getInAlluxioPercentage());
    }

    // Testing loading of a directory
    sFsShell.run("loadMetadata", "/testRoot");
    assertEquals(0, sFsShell.run("load", "/testRoot", "--submit", "--partial-listing"));
    assertEquals(0, sFsShell.run("load", "/testRoot", "--progress"));
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem,
        new AlluxioURI("/testRoot/testDirectory/testFile" + (numFiles - 1)), 100);
    while (!mOutput.toString().contains("SUCCEEDED")) {
      assertEquals(0, sFsShell.run("load", "/testRoot", "--progress"));
      Thread.sleep(1000);
    }
  }

  @Test
  public void testPartlyLoaded() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootPartial/testFileA",
        WritePType.THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootPartial/testFileB",
        WritePType.CACHE_THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootPartial/testDirectory/testFileC",
        WritePType.CACHE_THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootPartial/testDirectory/testFileD",
        WritePType.THROUGH, 100 * Constants.MB);
    AlluxioURI uriA = new AlluxioURI("/testRootPartial/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRootPartial/testFileB");
    AlluxioURI uriC = new AlluxioURI("/testRootPartial/testDirectory/testFileC");
    AlluxioURI uriD = new AlluxioURI("/testRootPartial/testDirectory/testFileD");
    assertEquals(0, sFileSystem.getStatus(uriA).getInAlluxioPercentage());
    assertEquals(100, sFileSystem.getStatus(uriB).getInAlluxioPercentage());
    assertEquals(100, sFileSystem.getStatus(uriC).getInAlluxioPercentage());
    assertEquals(0, sFileSystem.getStatus(uriD).getInAlluxioPercentage());
    // Testing loading of a directory
    sFsShell.run("loadMetadata", "/testRootLoaded");
    assertEquals(0, sFsShell.run("load", "/testRootPartial", "--submit"));
    assertEquals(0, sFsShell.run("load", "/testRootPartial", "--progress"));
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriA, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriB, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriC, 100);
    while (!mOutput.toString().contains("SUCCEEDED")) {
      assertEquals(0, sFsShell.run("load", "/testRootPartial", "--progress"));
      Thread.sleep(1000);
    }
  }

  @Test
  public void testAlreadyLoaded() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootLoaded/testFileA",
        WritePType.CACHE_THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootLoaded/testFileB",
        WritePType.CACHE_THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootLoaded/testDirectory/testFileC",
        WritePType.CACHE_THROUGH, Constants.MB);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRootLoaded/testDirectory/testFileD",
        WritePType.CACHE_THROUGH, 100 * Constants.MB);
    AlluxioURI uriA = new AlluxioURI("/testRootLoaded/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRootLoaded/testFileB");
    AlluxioURI uriC = new AlluxioURI("/testRootLoaded/testDirectory/testFileC");
    AlluxioURI uriD = new AlluxioURI("/testRootLoaded/testDirectory/testFileD");
    assertEquals(100, sFileSystem.getStatus(uriA).getInAlluxioPercentage());
    assertEquals(100, sFileSystem.getStatus(uriB).getInAlluxioPercentage());
    assertEquals(100, sFileSystem.getStatus(uriC).getInAlluxioPercentage());
    assertEquals(100, sFileSystem.getStatus(uriD).getInAlluxioPercentage());
    // Testing loading of a directory
    sFsShell.run("loadMetadata", "/testRootLoaded");
    assertEquals(0, sFsShell.run("load", "/testRootLoaded", "--submit"));
    assertEquals(0, sFsShell.run("load", "/testRootLoaded", "--progress"));
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriA, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriB, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriC, 100);
    while (!mOutput.toString().contains("SUCCEEDED")) {
      assertEquals(0, sFsShell.run("load", "/testRootLoaded", "--progress"));
      Thread.sleep(1000);
    }
    assertTrue(mOutput.toString().contains("Files Processed: 0 out of 0"));
    assertTrue(mOutput.toString().contains("Bytes Loaded: 0B out of 0B"));
  }
}

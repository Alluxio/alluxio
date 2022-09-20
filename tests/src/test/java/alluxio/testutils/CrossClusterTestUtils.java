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

package alluxio.testutils;

import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public final class CrossClusterTestUtils {

  public static final CreateFilePOptions CREATE_OPTIONS =
      CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();

  public static final CreateDirectoryPOptions CREATE_DIR_OPTIONS =
      CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();

  public static boolean fileExists(AlluxioURI path, FileSystem... fsArray) {
    for (FileSystem fs : fsArray) {
      try {
        fs.getStatus(path);
      } catch (FileDoesNotExistException e) {
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  /**
   * Check that files can be synchronized across all clusters.
   */
  public static void checkClusterSyncAcrossAll(
      AlluxioURI mountPath, FileSystem ... clients) throws Exception {
    for (FileSystem client : clients) {
      AlluxioURI file = mountPath.join(RandomStringUtils.randomAlphanumeric(20));
      assertFileDoesNotExist(file, clients);
      client.createFile(file, CREATE_OPTIONS).close();
      CommonUtils.waitFor("File synced across clusters",
          () -> fileExists(file, clients),
          WaitForOptions.defaults().setTimeoutMs(10_000));
    }
  }

  public static void assertFileDoesNotExist(AlluxioURI path, FileSystem ... fsArray) {
    for (FileSystem fs : fsArray) {
      assertThrows(FileDoesNotExistException.class,
          () -> fs.getStatus(path));
    }
  }

  public static void assertFileExists(AlluxioURI path, FileSystem ... fsArray)
      throws Exception {
    for (FileSystem fs : fsArray) {
      fs.getStatus(path);
    }
  }

  public static void checkNonCrossClusterWrite(
      String ufsPath, AlluxioURI mountPath, FileSystem client1, FileSystem client2)
      throws Exception {
    // ensure without cross cluster sync there is no visibility across clusters
    AlluxioURI file1 = new AlluxioURI("/file1");
    assertFileDoesNotExist(file1, client1, client2);
    client1.createFile(file1, CREATE_OPTIONS).close();
    assertFileExists(file1, client1);
    // be sure after a timeout the file still does not exist on cluster2
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File synced across clusters",
            () -> fileExists(file1, client2),
            WaitForOptions.defaults().setTimeoutMs(3000)));

    // Create a file on the UFS, it should not be visible in Alluxio since we
    // don't sync without an invalidation message
    String outsideFile = "createdOutsideAlluxio";
    AlluxioURI alluxioOutsideFile = mountPath.join(outsideFile);
    // first read the file to fill the absent cache
    assertFileDoesNotExist(alluxioOutsideFile, client1, client2);
    // create the file on the ufs
    Files.createFile(Paths.get(PathUtils.concatPath(ufsPath, outsideFile)));
    // be sure it is not in Alluxio after a timeout
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File synced across clusters",
            () -> {
              assertFileDoesNotExist(alluxioOutsideFile, client1, client2);
              return false;
            },
            WaitForOptions.defaults().setTimeoutMs(3000)));
  }

  private CrossClusterTestUtils() {} // This is an utils class not intended for instantiation
}

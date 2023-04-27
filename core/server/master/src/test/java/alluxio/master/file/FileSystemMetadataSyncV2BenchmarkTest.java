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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.mdsync.BaseTask;
import alluxio.util.CommonUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This class is to test the load metadata performance against a local UFS.
 * use {@link FileSystemMetadataSyncV2BenchmarkTest#generateTestFiles()} to generate test files
 * first, then run the v1 or v2 sync respectively.
 * This class is for debugging and should not be run as a unit test.
 */
@Ignore
public final class FileSystemMetadataSyncV2BenchmarkTest extends FileSystemMasterTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMetadataSyncV2BenchmarkTest.class);
  private static final String LOCAL_FS_ABSOLUTE_PATH = "/tmp/s3-test-files/bucket";
  private static final String SUB_DIR = "/0/0/0/0";
  private static final AlluxioURI UFS_ROOT = new AlluxioURI(
      "file://" + LOCAL_FS_ABSOLUTE_PATH + SUB_DIR);
  private static final AlluxioURI MOUNT_POINT = new AlluxioURI("/local_mount");

  @Override
  public void before() throws Exception {
    super.before();
  }

  @Test
  public void syncV2()
      throws Throwable {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());

    // Sync one file from UFS
    // First pass
    BaseTask result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, DirectoryLoadType.BFS, 0
    ).getBaseTask();
    result.waitComplete(0);
    System.out.println(result.getTaskInfo().getStats());

    System.out.println("--------Second pass----------");
    result = mFileSystemMaster.getMetadataSyncer().syncPath(
        MOUNT_POINT, DescendantType.ALL, DirectoryLoadType.BFS, 0
    ).getBaseTask();
    result.waitComplete(0);
    System.out.println(result.getTaskInfo().getStats());
  }

  @Test
  public void syncV1()
      throws FileDoesNotExistException, FileAlreadyExistsException, AccessControlException,
      IOException, InvalidPathException {
    mFileSystemMaster.mount(MOUNT_POINT, UFS_ROOT, MountContext.defaults());

    // Sync one file from UFS
    long start = CommonUtils.getCurrentMs();
    mFileSystemMaster.listStatus(MOUNT_POINT, listSync(true));
    System.out.println("Time elapsed " + (CommonUtils.getCurrentMs() - start) + "ms");
  }

  @Ignore
  @Test
  public void generateTestFiles() throws IOException {
    int count = 0;
    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 2; ++j) {
        for (int k = 0; k < 2; ++k) {
          for (int l = 0; l < 2; ++l) {
            for (int n = 0; n < 10; ++n) {
              for (int m = 0; m < 10000; ++m) {
                count++;
                if (count % 10000 == 0) {
                  System.out.println(count);
                }
                String fileData = "f";
                FileOutputStream fos =
                    FileUtils.openOutputStream(new File(
                        String.format(
                            "%s/%d/%d/%d/%d/%d/f%d", LOCAL_FS_ABSOLUTE_PATH, i, j, k, l, n, m)));
                fos.write(fileData.getBytes());
                fos.flush();
                fos.close();
              }
            }
          }
        }
      }
    }
  }

  private ListStatusContext listSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
        .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.ALWAYS)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build()
        ));
  }
}

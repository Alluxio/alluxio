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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

/**
 * Integration tests for handling file TTLs (times to live).
 */
public class TtlIntegrationTest extends BaseIntegrationTest {
  private static final int TTL_INTERVAL_MS = 50;

  private FileSystem mFileSystem;

  private FileOutStream mOutStream = null;

  protected byte[] mBuffer;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public File mUfs = AlluxioTestDirectory.createTemporaryDirectory("RootUfs");
  private String mLocalUfsPath = mUfs.getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
              .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mLocalUfsPath)
              .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, TTL_INTERVAL_MS)
              .setProperty(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataPType.NEVER)
              .build();

  @Before
  public void before() throws Exception {
    mFileSystem = FileSystem.Factory.create();
    mBuffer = new byte[10];
    Arrays.fill(mBuffer, (byte) 'A');
  }

  /**
   * Tests that when many TTLs expire at the same time, files are deleted from alluxio properly.
   */
  @Test
  public void expireManyAfterDeleteAlluxio() throws Exception {
    int numFiles = 100;
    AlluxioURI[] files = new AlluxioURI[numFiles];
    for (int i = 0; i < numFiles; i++) {
      files[i] = new AlluxioURI("/file" + i);
      // Only the even-index files should expire.
      long ttl = i % 2 == 0 ? TTL_INTERVAL_MS / 2 : TTL_INTERVAL_MS * 1000;
      mOutStream = mFileSystem.createFile(files[i],
              CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
                      .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)
                      .setTtlAction(TtlAction.DELETE_ALLUXIO)).build());
      mOutStream.write(mBuffer, 0, 10);
      mOutStream.close();

      // Delete some of the even files to make sure this doesn't trip up the TTL checker.
      if (i % 20 == 0) {
        mFileSystem.delete(files[i]);
      }
    }
    CommonUtils.sleepMs(2 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    for (int i = 0; i < numFiles; i++) {
      if (i % 2 == 0) {
        assertFalse(mFileSystem.exists(files[i]));
        // Check Ufs file existence
        if (i % 20 != 0) {
          String fileName = "file" + i;
          // Check Ufs file existence
          assertTrue(Arrays.stream(mUfs.list()).anyMatch(s -> s.equals(fileName)));
        }
      } else {
        assertTrue(mFileSystem.exists(files[i]));
      }
    }
  }

  /**
   * Tests that when many TTLs expire at the same time, files are freed properly.
   */
  @Test
  public void expireManyAfterFree() throws Exception {
    int numFiles = 100;
    AlluxioURI[] files = new AlluxioURI[numFiles];
    for (int i = 0; i < numFiles; i++) {
      files[i] = new AlluxioURI("/fileFree" + i);
      // Only the even-index files should expire.
      long ttl = i % 2 == 0 ? TTL_INTERVAL_MS / 2 : TTL_INTERVAL_MS * 1000;
      mOutStream = mFileSystem.createFile(files[i],
          CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
              .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl))
                  .build());
      mOutStream.write(mBuffer, 0, 10);
      mOutStream.close();

      // Delete some of the even files to make sure this doesn't trip up the TTL checker.
      if (i % 20 == 0) {
        mFileSystem.delete(files[i]);
      }
    }
    CommonUtils.sleepMs(2 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Sleep for a while to make sure the delete operations are done.
    CommonUtils.sleepMs(4 * TTL_INTERVAL_MS);
    for (int i = 0; i < numFiles; i++) {
      if (i % 2 == 0) {
        if (i % 20 != 0) {
          assertEquals(Constants.NO_TTL, mFileSystem.getStatus(files[i]).getTtl());
          assertEquals(TtlAction.DELETE, mFileSystem.getStatus(files[i]).getTtlAction());
          assertEquals(0, mFileSystem.getStatus(files[i]).getInMemoryPercentage());
          String fileName = "fileFree" + i;
          // Check Ufs file existence
          assertTrue(Arrays.stream(mUfs.list()).anyMatch(s -> s.equals(fileName)));
        }
      } else {
        assertTrue(mFileSystem.exists(files[i]));
        assertEquals(100, mFileSystem.getStatus(files[i]).getInMemoryPercentage());
      }
    }
  }

  /**
   * Tests that when many TTLs expire at the same time, files are deleted from Alluxio and
   * UFS properly.
   */
  @Test
  public void expireManyAfterDelete() throws Exception {
    int numFiles = 100;
    AlluxioURI[] files = new AlluxioURI[numFiles];
    for (int i = 0; i < numFiles; i++) {
      files[i] = new AlluxioURI("/fileDelete" + i);
      // Only the even-index files should expire.
      long ttl = i % 2 == 0 ? TTL_INTERVAL_MS / 2 : TTL_INTERVAL_MS * 1000;
      mOutStream = mFileSystem.createFile(files[i],
              CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
                      .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)
                              .setTtlAction(TtlAction.DELETE)).build());
      mOutStream.write(mBuffer, 0, 10);
      mOutStream.close();

      // Delete some of the even files to make sure this doesn't trip up the TTL checker.
      if (i % 20 == 0) {
        mFileSystem.delete(files[i]);
      }
    }
    CommonUtils.sleepMs(4 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    for (int i = 0; i < numFiles; i++) {
      if (i % 2 == 0) {
        assertFalse(mFileSystem.exists(files[i]));
        String fileName = "fileDelete" + i;
        // Check Ufs file existence
        assertFalse(Arrays.stream(mUfs.list()).anyMatch(s -> s.equals(fileName)));
      } else {
        assertTrue(mFileSystem.exists(files[i]));
      }
    }
  }

  /**
   * Tests that ttl on a directory will be enforced on all its children regarless
   * of their ttl.
   * @throws Exception
   */
  @Test
  public void expireADirectory() throws Exception {
    int numFiles = 100;
    AlluxioURI[] files = new AlluxioURI[numFiles];
    String directoryName = "dir1";
    mFileSystem.createDirectory(new AlluxioURI("/" + directoryName),
        CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build());
    String fileNamePrefix = "fileDelete";
    for (int i = 0; i < numFiles; i++) {
      files[i] = new AlluxioURI("/" + directoryName + "/" + fileNamePrefix + i);
      // Only the even-index files should expire.
      long ttl = i % 2 == 0 ? TTL_INTERVAL_MS * 2000 : TTL_INTERVAL_MS * 1000;
      mOutStream = mFileSystem.createFile(files[i],
          CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
              .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)
                  .setTtlAction(TtlAction.DELETE)).build());
      mOutStream.write(mBuffer, 0, 10);
      mOutStream.close();
    }
    // Set much smaller ttl on directory.
    SetAttributePOptions setTTlOptions = SetAttributePOptions.newBuilder().setRecursive(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(TTL_INTERVAL_MS).setTtlAction(TtlAction.DELETE).build())
        .build();
    mFileSystem.setAttribute(new AlluxioURI("/" + directoryName), setTTlOptions);
    // Individual children file's ttl should not be changed.
    Random random = new Random();
    int fileNum = random.nextInt(numFiles);
    URIStatus anyFileStatus = mFileSystem.getStatus(new AlluxioURI("/" + directoryName
        + "/" + fileNamePrefix + fileNum));
    assert (anyFileStatus.getFileInfo().getTtl()
        == (fileNum % 2 == 0 ? TTL_INTERVAL_MS * 2000 : TTL_INTERVAL_MS * 1000));

    CommonUtils.sleepMs(4 * TTL_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    /* Even though children have longer ttl, but parents' ttl overrides all.
    No Children should exist now. */
    for (int i = 0; i < numFiles; i++) {
      assertFalse(mFileSystem.exists(files[i]));
      String fileName = directoryName + "/" + fileNamePrefix + i;
      // Check Ufs file existence
      assertFalse(Arrays.stream(mUfs.list()).anyMatch(s -> s.equals(fileName)));
    }
  }
}

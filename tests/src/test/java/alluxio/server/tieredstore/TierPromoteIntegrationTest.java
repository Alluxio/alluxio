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

package alluxio.server.tieredstore;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TierPromoteIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCKS_PER_TIER = 10;
  private static final String BLOCK_SIZE_BYTES = "1KB";
  private static final long CAPACITY_BYTES =
      BLOCKS_PER_TIER * FormatUtils.parseSpaceSize(BLOCK_SIZE_BYTES);
  private static final WaitForOptions WAIT_OPTIONS =
      WaitForOptions.defaults().setTimeoutMs(2000).setInterval(10);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;

  private FileSystem mFileSystem = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[] { /* short circuit enabled */ "true" });
    list.add(new Object[] { /* short circuit enabled */ "false"});
    return list;
  }

  /**
   * Constructor.
   *
   * @param shortCircuitEnabled whether to enable the short circuit data paths
   */
  public TierPromoteIntegrationTest(String shortCircuitEnabled) {
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, CAPACITY_BYTES)
        .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, shortCircuitEnabled)
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
        .setProperty(PropertyKey.WORKER_MANAGEMENT_LOAD_DETECTION_COOL_DOWN_TIME, "2s")
        .setProperty(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, "false")
        .setProperty(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS
            .format(1), Constants.MEDIUM_SSD)
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
            Files.createTempDir().getAbsolutePath())
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
            Files.createTempDir().getAbsolutePath())
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
            String.valueOf(CAPACITY_BYTES))
        .setProperty(PropertyKey.WORKER_REVIEWER_CLASS,
            "alluxio.worker.block.reviewer.AcceptingReviewer")
        .build();
  }

  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE"})
  @Test
  public void promoteByTierSwap() throws Exception {
    final int size = (int) CAPACITY_BYTES / 2;
    AlluxioURI path1 = new AlluxioURI(PathUtils.uniqPath());
    AlluxioURI path2 = new AlluxioURI(PathUtils.uniqPath());

    // Write two files, first file should be in memory, the second should be in ssd tier.
    FileOutStream os1 = mFileSystem.createFile(path1,
        CreateFilePOptions.newBuilder().setWriteTier(0).setRecursive(true).build());
    os1.write(BufferUtils.getIncreasingByteArray(size));
    os1.close();
    FileOutStream os2 = mFileSystem.createFile(path2,
        CreateFilePOptions.newBuilder().setWriteTier(1).setRecursive(true).build());
    os2.write(BufferUtils.getIncreasingByteArray(size));
    os2.close();

    // Not in memory but in Alluxio storage
    CommonUtils.waitFor("file is not in memory", () -> {
      try {
        return 0 == mFileSystem.getStatus(path2).getInMemoryPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
    CommonUtils.waitFor("file has block locations", () -> {
      try {
        return !mFileSystem.getStatus(path1).getFileBlockInfos().isEmpty();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);

    // After reading the second file, it should be moved to memory tier as per LRU.
    FileInStream in = mFileSystem.openFile(path2, OpenFilePOptions.getDefaultInstance());
    byte[] buf = new byte[size];
    while (in.read(buf) != -1) {
      // read the entire file
    }
    in.close();

    CommonUtils.waitFor("File getting promoted to memory tier.", () -> {
      try {
        // In memory
        return 100 == mFileSystem.getStatus(path2).getInMemoryPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(60000));
  }

  // Disable tier management tasks.
  // Set is to MUST_CACHE to make blocks evictable.
  // No reserved-space for precise capacity planning.
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, "false",
          PropertyKey.Name.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, "false",
          PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE"})
  @Test
  public void promoteByRead() throws Exception {
    final int size = (int) CAPACITY_BYTES / 2;
    AlluxioURI path1 = new AlluxioURI(PathUtils.uniqPath());

    // Write a file to ssd tier.
    FileOutStream os1 = mFileSystem.createFile(path1,
            CreateFilePOptions.newBuilder().setWriteTier(1).setRecursive(true).build());
    os1.write(BufferUtils.getIncreasingByteArray(size));
    os1.close();

    // Fill mem tier.
    long fileBytes = CAPACITY_BYTES / 2;
    for (int i = 0; i < 2; i++) {
      FileOutStream os = mFileSystem.createFile(new AlluxioURI(PathUtils.uniqPath()),
          CreateFilePOptions.newBuilder().setWriteTier(0).setRecursive(true).build());
      os.write(BufferUtils.getIncreasingByteArray((int) fileBytes));
      os.close();
    }

    // Not in memory but in Alluxio storage
    CommonUtils.waitFor("file is not in memory", () -> {
      try {
        return 0 == mFileSystem.getStatus(path1).getInMemoryPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
    CommonUtils.waitFor("file has block locations", () -> {
      try {
        return !mFileSystem.getStatus(path1).getFileBlockInfos().isEmpty();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);

    // Before reading the second file, it should be moved to memory tier as per read flag.
    FileInStream in = mFileSystem.openFile(path1,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build());
    byte[] buf = new byte[size];
    while (in.read(buf) != -1) {
      // read the entire file
    }

    in.close();
    // In memory
    CommonUtils.waitFor("file is in memory", () -> {
      try {
        return 100 == mFileSystem.getStatus(path1).getInMemoryPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }
}

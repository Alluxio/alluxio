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
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FormatUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
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

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

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
        .setProperty(PropertyKey.WORKER_FILE_BUFFER_SIZE, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, CAPACITY_BYTES)
        .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, shortCircuitEnabled)
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1), "SSD")
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
            Files.createTempDir().getAbsolutePath())
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(1),
            Files.createTempDir().getAbsolutePath())
        .setProperty(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA.format(1),
            String.valueOf(CAPACITY_BYTES)).build();
  }

  @Test
  public void promoteBlock() throws Exception {
    final int size = (int) CAPACITY_BYTES / 2;
    AlluxioURI path1 = new AlluxioURI(PathUtils.uniqPath());
    AlluxioURI path2 = new AlluxioURI(PathUtils.uniqPath());
    AlluxioURI path3 = new AlluxioURI(PathUtils.uniqPath());

    // Write three files, first file should be in ssd, the others should be in memory
    FileOutStream os1 =
        mFileSystem.createFile(path1, CreateFilePOptions.newBuilder().setRecursive(true).build());
    os1.write(BufferUtils.getIncreasingByteArray(size));
    os1.close();
    FileOutStream os2 =
        mFileSystem.createFile(path2, CreateFilePOptions.newBuilder().setRecursive(true).build());
    os2.write(BufferUtils.getIncreasingByteArray(size));
    os2.close();
    FileOutStream os3 =
        mFileSystem.createFile(path3, CreateFilePOptions.newBuilder().setRecursive(true).build());
    os3.write(BufferUtils.getIncreasingByteArray(size));
    os3.close();

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // Not in memory but in Alluxio storage
    Assert.assertEquals(0, mFileSystem.getStatus(path1).getInMemoryPercentage());
    Assert.assertFalse(mFileSystem.getStatus(path1).getFileBlockInfos().isEmpty());

    // After reading with CACHE_PROMOTE, the file should be in memory
    FileInStream in = mFileSystem.openFile(path1,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build());
    byte[] buf = new byte[size];
    while (in.read(buf) != -1) {
      // read the entire file
    }
    in.close();

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    // In memory
    Assert.assertEquals(100, mFileSystem.getStatus(path1).getInMemoryPercentage());
  }
}

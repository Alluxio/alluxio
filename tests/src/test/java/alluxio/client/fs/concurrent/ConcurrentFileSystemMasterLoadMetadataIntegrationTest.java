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

package alluxio.client.fs.concurrent;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Tests loading UFS metadata many times concurrently.
 */
public final class ConcurrentFileSystemMasterLoadMetadataIntegrationTest {
  private static final int CONCURRENCY_FACTOR = 20;
  private FileSystem mFileSystem;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          .setProperty(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          /**
           * This is to make sure master executor has enough thread to being with. Otherwise, delay
           * on master ForkJoinPool's internal thread count adjustment might take several seconds.
           * This can interfere with this test's timing expectations.
           */
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, CONCURRENCY_FACTOR)
          .build();

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
  }

  @Test
  public void loadMetadataManyDirectories() throws Exception {
    String ufsPath = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    for (int i = 0; i < 5000; i++) {
      Files.createDirectory(Paths.get(ufsPath, "a" + i));
    }

    // Run concurrent listStatus calls on the root.
    List<AlluxioURI> paths = Collections.nCopies(CONCURRENCY_FACTOR, new AlluxioURI("/"));

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.LIST_STATUS,
            paths.toArray(new AlluxioURI[]{}), 60 * Constants.SECOND_MS);
    assertEquals(Collections.EMPTY_LIST, errors);
  }

  @Test
  public void loadMetadataManyFiles() throws Exception {
    String ufsPath = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    for (int i = 0; i < 5000; i++) {
      Files.createFile(Paths.get(ufsPath, "a" + i));
    }

    // Run concurrent listStatus calls on the root.
    List<AlluxioURI> paths = Collections.nCopies(CONCURRENCY_FACTOR, new AlluxioURI("/"));

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.LIST_STATUS,
            paths.toArray(new AlluxioURI[]{}), 60 * Constants.SECOND_MS);
    assertEquals(Collections.EMPTY_LIST, errors);
  }
}

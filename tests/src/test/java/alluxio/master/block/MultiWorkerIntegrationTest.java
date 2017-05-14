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

package alluxio.master.block;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.policy.RoundRobinPolicy;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests a cluster containing multiple workers.
 */
public final class MultiWorkerIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_WORKERS = 4;
  private static final int WORKER_MEMORY_SIZE_BYTES = Constants.MB;
  private static final int BLOCK_SIZE_BYTES = WORKER_MEMORY_SIZE_BYTES / 2;

  @Rule
  public LocalAlluxioClusterResource mResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_MEMORY_SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BLOCK_SIZE_BYTES)
          .setNumWorkers(NUM_WORKERS)
          .build();

  @Test
  public void writeLargeFile() throws Exception {
    int fileSize = NUM_WORKERS * WORKER_MEMORY_SIZE_BYTES;
    AlluxioURI file = new AlluxioURI("/test");
    FileSystem fs = mResource.get().getClient();
    // Write a file large enough to fill all the memory of all the workers.
    FileSystemTestUtils.createByteFile(fs, file.getPath(), fileSize,
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
            .setLocationPolicy(new RoundRobinPolicy()));
    URIStatus status = fs.getStatus(file);
    assertEquals(100, status.getInMemoryPercentage());
    try (FileInStream inStream = fs.openFile(file)) {
      assertEquals(fileSize, IOUtils.toByteArray(inStream).length);
    }
  }
}

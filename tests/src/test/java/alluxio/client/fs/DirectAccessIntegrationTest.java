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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

/**
 * Integration tests for direct access configurations.
 */
public class DirectAccessIntegrationTest extends BaseIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final String DIRECT_DIR = "/mnt/direct/";
  private static final String DIRECT_DIR_REGEX = ".?/mnt/direct/.?";
  private static final String NON_DIRECT_DIR = "/mnt/non_direct/";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.USER_FILE_DIRECT_ACCESS, DIRECT_DIR_REGEX)
          .build();
  private FileSystem mFileSystem;

  private final String mLocalUfsPath = AlluxioTestDirectory
      .createTemporaryDirectory("DirectAccessIntegrationTest").getAbsolutePath();

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));
  }

  @Test
  public void writeDirect() throws Exception {
    final int n = 3;
    for (int i = 0; i < n; i++) {
      AlluxioURI uri = new AlluxioURI(DIRECT_DIR + i);
      try (FileOutStream os = mFileSystem.createFile(uri,
          CreateFilePOptions.newBuilder().setRecursive(true).build())) {
        os.write(TEST_BYTES);
      }
    }
    Assert.assertEquals(n, new File(mLocalUfsPath + "/direct").listFiles().length);
    for (int i = 0; i < n; i++) {
      checkCacheStatus(DIRECT_DIR + i, false, false);
    }
  }

  @Test
  public void writeNonDirect() throws Exception {
    final int n = 3;
    for (int i = 0; i < n; i++) {
      AlluxioURI uri = new AlluxioURI(NON_DIRECT_DIR + i);
      try (FileOutStream os = mFileSystem.createFile(uri,
          CreateFilePOptions.newBuilder().setRecursive(true).build())) {
        os.write(TEST_BYTES);
      }
    }
    Assert.assertNull(new File(mLocalUfsPath + "/non_direct").listFiles());

    for (int i = 0; i < n; i++) {
      checkCacheStatus(NON_DIRECT_DIR + i, true, true);
    }
  }

  private void checkCacheStatus(String path,
      boolean shouldCacheBefore, boolean shouldCache) throws Exception {
    AlluxioURI uri = new AlluxioURI(path);
    Assert.assertEquals(shouldCacheBefore ? 100 : 0,
        mFileSystem.getStatus(uri).getInMemoryPercentage());
    try (FileInStream is = mFileSystem.openFile(uri)) {
      IOUtils.copy(is, ByteStreams.nullOutputStream());
    }
    // Find the block location directly from block info to determine
    // if the file has been cached
    Assert.assertTrue(shouldCache ^ mFileSystem.getBlockLocations(uri)
        .get(0).getBlockInfo().getBlockInfo().getLocations().isEmpty());
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, shouldCache ? 100 : 0);
    Assert.assertEquals(shouldCache ? 100 : 0, mFileSystem.getStatus(uri).getInMemoryPercentage());
  }
}

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

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for path level configurations.
 */
public class PathConfigurationIntegrationTest {
  private static final byte[] TEST_BYTES = "TestBytes".getBytes();
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final String REMOTE_DIR = "/remote/";
  private static final String REMOTE_UNCACHED_FILE = "/remote/0";
  private static final String LOCAL_DIR = "/local/";
  private static final String LOCAL_CACHED_FILE = "/local/0";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.PATHS, 4)
          .setProperty(PropertyKey.Template.PATH_INDEX.format(0), REMOTE_DIR)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(0,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.CACHE.toString())
          .setProperty(PropertyKey.Template.PATH_INDEX.format(1), REMOTE_UNCACHED_FILE)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(1,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.NO_CACHE.toString())
          .setProperty(PropertyKey.Template.PATH_INDEX.format(2), LOCAL_DIR)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(2,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.NO_CACHE.toString())
          .setProperty(PropertyKey.Template.PATH_INDEX.format(3), LOCAL_CACHED_FILE)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(3,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.CACHE.toString())
          .build();
  private FileSystem mFileSystem = null;
  private CreateFilePOptions mWriteThrough;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteThrough = CreateFilePOptions.newBuilder().setRecursive(true)
        .setWriteType(WritePType.THROUGH).build();
  }

  @Test
  public void read() throws Exception {
    final int n = 3;

    String[] dirs = new String[]{REMOTE_DIR, LOCAL_DIR};
    for (String dir : dirs) {
      for (int i = 0; i < n; i++) {
        AlluxioURI uri = new AlluxioURI(dir + i);
        try (FileOutStream os = mFileSystem.createFile(uri, mWriteThrough)) {
          os.write(TEST_BYTES);
        }
      }
    }

    checkCacheStatus(REMOTE_UNCACHED_FILE, false);
    for (int i = 1; i < n; i++) {
      checkCacheStatus(REMOTE_DIR + i, true);
    }

    checkCacheStatus(LOCAL_CACHED_FILE, true);
    for (int i = 1; i < n; i++) {
      checkCacheStatus(LOCAL_DIR + i, false);
    }
  }

  private void checkCacheStatus(String path, boolean shouldCache) throws Exception {
    AlluxioURI uri = new AlluxioURI(path);
    Assert.assertEquals(0, mFileSystem.getStatus(uri).getInMemoryPercentage());
    try (FileInStream is = mFileSystem.openFile(uri)) {
      IOUtils.copy(is, ByteStreams.nullOutputStream());
    }
    Assert.assertEquals(shouldCache ? 100 : 0, mFileSystem.getStatus(uri).getInMemoryPercentage());
  }
}

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
  private static final String LOCAL_DIR = "/local/";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.PATHS, 2)
          .setProperty(PropertyKey.Template.PATH_INDEX.format(0), REMOTE_DIR)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(0,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.CACHE_PROMOTE.toString())
          .setProperty(PropertyKey.Template.PATH_INDEX.format(1), LOCAL_DIR)
          .setProperty(PropertyKey.Template.PATH_PROPERTY.format(1,
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT), ReadType.NO_CACHE.toString())
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

    for (int i = 0; i < n; i++) {
      AlluxioURI uri = new AlluxioURI(REMOTE_DIR + i);
      Assert.assertEquals(0, mFileSystem.getStatus(uri).getInMemoryPercentage());
      try (FileInStream is = mFileSystem.openFile(uri)) {
        IOUtils.copy(is, ByteStreams.nullOutputStream());
      }
      Assert.assertEquals(100, mFileSystem.getStatus(uri).getInMemoryPercentage());
    }

    for (int i = 0; i < n; i++) {
      AlluxioURI uri = new AlluxioURI(LOCAL_DIR + i);
      Assert.assertEquals(0, mFileSystem.getStatus(uri).getInMemoryPercentage());
      try (FileInStream is = mFileSystem.openFile(uri)) {
        IOUtils.copy(is, ByteStreams.nullOutputStream());
      }
      Assert.assertEquals(0, mFileSystem.getStatus(uri).getInMemoryPercentage());
    }
  }
}

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
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
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
          .build();
  private MetaMasterConfigClient mMetaConfig;
  private FileSystem mFileSystem;
  private CreateFilePOptions mWriteThrough;

  private void setPathConfigurations(MetaMasterConfigClient client) throws Exception {
    client.setPathConfiguration(new AlluxioURI(REMOTE_DIR), PropertyKey.USER_FILE_READ_TYPE_DEFAULT,
        ReadType.CACHE.toString());
    client.setPathConfiguration(new AlluxioURI(REMOTE_UNCACHED_FILE),
        PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.NO_CACHE.toString());
    client.setPathConfiguration(new AlluxioURI(LOCAL_DIR), PropertyKey.USER_FILE_READ_TYPE_DEFAULT,
        ReadType.NO_CACHE.toString());
    client.setPathConfiguration(new AlluxioURI(LOCAL_CACHED_FILE),
        PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.CACHE.toString());
  }

  @Before
  public void before() throws Exception {
    FileSystemContext metaCtx = FileSystemContext.create(ServerConfiguration.global());
    mMetaConfig = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(metaCtx.getClientContext()).build());
    setPathConfigurations(mMetaConfig);
    FileSystemContext fsCtx = FileSystemContext.create(ServerConfiguration.global());
    fsCtx.getClientContext().loadConf(fsCtx.getMasterAddress(), true, true);
    mFileSystem = mLocalAlluxioClusterResource.get().getClient(fsCtx);
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
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uri, shouldCache ? 100 : 0);
  }
}

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

package alluxio.client.fs.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.InputStream;

public class DoraReadIntegrationTest extends BaseIntegrationTest {
  private static final int TEST_FILE_LENGTH = 50 * Constants.KB;

  private static final String UFS_ROOT =
      AlluxioTestDirectory.createTemporaryDirectory("ufs_root").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, UFS_ROOT)
          .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
          .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
          .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private FileSystemContext mFsContext;
  private FileSystem mFileSystem;
  private String mFilePath;

  @Before
  public void before() throws Exception {
    mFsContext = FileSystemContext.create(mClusterResource.get().getClient().getConf());
    mFileSystem = mClusterResource.get().getClient(mFsContext);
    mFilePath =  UFS_ROOT + PathUtils.uniqPath();
  }

  @Test
  public void read() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, TEST_FILE_LENGTH);
    // read a file to populate the cache
    try (FileInStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          TEST_FILE_LENGTH, ByteStreams.toByteArray(stream)));
    }
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          TEST_FILE_LENGTH, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void positionedRead() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, TEST_FILE_LENGTH);
    try (FileInStream stream = mFileSystem.openFile(path)) {
      byte[] buffer = new byte[TEST_FILE_LENGTH / 4];
      int bytesRead = stream.positionedRead(TEST_FILE_LENGTH / 10, buffer, 0, buffer.length);
      assertEquals(buffer.length, bytesRead);
      assertTrue(BufferUtils.equalIncreasingByteArray(TEST_FILE_LENGTH / 10,
          buffer.length, buffer));
    }
    // verify reading whole page from local cache
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          TEST_FILE_LENGTH, ByteStreams.toByteArray(stream)));
    }
  }
}

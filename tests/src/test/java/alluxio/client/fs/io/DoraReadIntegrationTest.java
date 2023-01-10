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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DoraReadIntegrationTest extends BaseIntegrationTest {

  private static final String UFS_ROOT =
      AlluxioTestDirectory.createTemporaryDirectory("ufs_root").getAbsolutePath();

  @Parameterized.Parameters(name = "{index}_FileLength_{0}")
  public static Collection testParams() {
    return Arrays.asList(new Object[][] {
        { 0},
        { 1},
        { 1 * Constants.KB - 1},
        { 1 * Constants.KB},
        { 1 * Constants.KB + 1},
        { 64 * Constants.KB - 1},
        { 64 * Constants.KB},
        { 64 * Constants.KB + 1},
    });
  }

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, UFS_ROOT)
          .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
          .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
          .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
          .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private FileSystemContext mFsContext;
  private FileSystem mFileSystem;
  private String mFilePath;

  private int mFileLength;

  public DoraReadIntegrationTest(int fileLength) {
    mFileLength = fileLength;
  }

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
        mFileSystem, mFilePath, WritePType.MUST_CACHE, mFileLength);
    // read a file to populate the cache
    try (FileInStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          mFileLength, ByteStreams.toByteArray(stream)));
    }
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          mFileLength, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void positionedRead() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, mFileLength);
    try (FileInStream stream = mFileSystem.openFile(path)) {
      byte[] buffer = new byte[mFileLength / 4];
      int bytesRead = stream.positionedRead(mFileLength / 10, buffer, 0, buffer.length);
      assertEquals(buffer.length, bytesRead);
      assertTrue(BufferUtils.equalIncreasingByteArray(mFileLength / 10,
          buffer.length, buffer));
    }
    // verify reading whole page from local cache
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          mFileLength, ByteStreams.toByteArray(stream)));
    }
  }
}

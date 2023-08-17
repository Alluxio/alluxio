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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.PositionReaderTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DoraPositionReaderIntegrationTest extends BaseIntegrationTest {

  private static final String UFS_ROOT =
      AlluxioTestDirectory.createTemporaryDirectory("ufs_root").getAbsolutePath();

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {0},
        {1},
        {128},
        {256},
        {666},
        {5314},
        { Constants.KB - 1},
        { Constants.KB},
        { Constants.KB + 1},
        { 64 * Constants.KB - 1},
        { 64 * Constants.KB},
        { 64 * Constants.KB + 1},
    });
  }

  @Parameterized.Parameter
  public int mFileLen;

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, "TRANSFER")
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, UFS_ROOT)
          .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
          .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
          .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
          .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
          .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "1GB")
          .setProperty(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED, true)
          .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private FileSystem mFileSystem;
  private String mFilePath;
  private PositionReader mPositionReader;
  private PositionReaderTest mPositionReaderTest;

  @Before
  public void before() throws Exception {
    FileSystemContext context = FileSystemContext
        .create(mClusterResource.get().getClient().getConf());
    mFileSystem = mClusterResource.get().getClient(context);
    mFilePath =  UFS_ROOT + PathUtils.uniqPath();
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.CACHE_THROUGH, mFileLen);
    mPositionReader = mFileSystem.openPositionRead(new AlluxioURI(mFilePath));
    mPositionReaderTest = new PositionReaderTest(mPositionReader, mFileLen);
  }

  @After
  public void after() throws Exception {
    mPositionReader.close();
    mFileSystem.close();
    new File(mFilePath).delete();
  }

  @Test
  public void testAllCornerCases() throws IOException {
    mPositionReaderTest.testAllCornerCases();
  }

  @Test
  public void testReadRandomPart() throws IOException {
    mPositionReaderTest.testReadRandomPart();
  }

  @Test
  public void testConcurrentReadRandomPart() throws Exception {
    mPositionReaderTest.concurrentReadPart();
  }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;

public class HdfsUfsDisableDoraIntegrationTest {
  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();
  protected final Configuration mHdfsConfiguration = new Configuration();
  private static final int HDFS_NAMENODE_PORT = 9870;
  private static final String BASE_NAMESPACE_URI = "hdfs://localhost:" + HDFS_NAMENODE_PORT + "/";
  private static final String TEST_CONTENT = "test-content";

  protected MiniDFSCluster mCluster;
  private FileSystem mAlluxioHdfsClient;

  private static final int BLOCK_SIZE = 1024 * 1024;

  @Before
  public void before() throws Exception {
    mHdfsConfiguration.set("dfs.name.dir", mTemp.newFolder("nn").getAbsolutePath());
    mHdfsConfiguration.set("dfs.data.dir", mTemp.newFolder("dn").getAbsolutePath());
    // 1MB block size for testing to save memory
    mHdfsConfiguration.setInt("dfs.block.size", BLOCK_SIZE);

    mCluster = new MiniDFSCluster.Builder(mHdfsConfiguration)
        .enableManagedDfsDirsRedundancy(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .nameNodePort(HDFS_NAMENODE_PORT)
        .numDataNodes(1).build();

    Configuration clientConfiguration = new Configuration();
    clientConfiguration.set("fs.s3.impl", "alluxio.hadoop.FileSystem");
    clientConfiguration.set("fs.AbstractFileSystem.s3.impl", "alluxio.hadoop.AlluxioFileSystem");
    clientConfiguration.setBoolean(PropertyKey.DORA_ENABLED.getName(), false);

    mAlluxioHdfsClient = FileSystem.get(new URI(BASE_NAMESPACE_URI), clientConfiguration);
  }

  @After
  public void after() {
    if (mCluster != null) {
      mCluster.close();
    }
  }

  @Test
  public void testReadWrite() throws Exception {
    Path testPath = new Path(BASE_NAMESPACE_URI + "test-file");
    FSDataOutputStream out = mAlluxioHdfsClient.create(testPath);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mAlluxioHdfsClient.open(testPath);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    assertEquals(Arrays.toString(buf), Arrays.toString(buffReadFromDora));
    mAlluxioHdfsClient.delete(testPath, false);
  }

  @Test
  public void testGetFileStatus() throws Exception {
    Path testPath = new Path(BASE_NAMESPACE_URI + "test-file");
    FSDataOutputStream out = mAlluxioHdfsClient.create(testPath);
    byte[] buf = TEST_CONTENT.getBytes(Charset.defaultCharset());
    out.write(buf);
    out.close();

    FSDataInputStream in = mAlluxioHdfsClient.open(testPath);
    byte[] buffReadFromDora = new byte[buf.length];
    in.read(buffReadFromDora);
    in.close();
    FileStatus fileStatus = mAlluxioHdfsClient.getFileStatus(testPath);
    assertEquals(buf.length, fileStatus.getLen());
    BlockLocation[] blockLocations =
        mAlluxioHdfsClient.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    assertEquals("127.0.0.1", blockLocations[0].getHosts()[0]);
    mAlluxioHdfsClient.delete(testPath, false);
  }

  @Test
  public void testListStatus() throws Exception {
    String testDirName = "testdir";
    String testFileName = "testfile";
    Path fullDirPath = new Path(BASE_NAMESPACE_URI + testDirName);
    Path fullFilePath = new Path(BASE_NAMESPACE_URI + testFileName);
    mAlluxioHdfsClient.mkdirs(fullDirPath);
    FSDataOutputStream out = mAlluxioHdfsClient.create(fullFilePath);
    out.close();

    Path pathToList = new Path(BASE_NAMESPACE_URI);
    FileStatus[] fileStatuses = mAlluxioHdfsClient.listStatus(pathToList);
    assertEquals(fullDirPath, fileStatuses[0].getPath());
    assertTrue(fileStatuses[0].isDirectory());
    assertEquals(fullFilePath, fileStatuses[1].getPath());
    assertFalse(fileStatuses[1].isDirectory());

    mAlluxioHdfsClient.delete(fullDirPath, true);
    mAlluxioHdfsClient.delete(fullFilePath, false);
  }
}

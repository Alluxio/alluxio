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

package alluxio.underfs.hdfs.hdfs3;

import static junit.framework.TestCase.assertEquals;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsUnderFileSystemIntegrationTestBase {
  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();
  protected final Configuration mHdfsConfiguration = new Configuration();

  protected MiniDFSCluster mCluster;
  protected HdfsUnderFileSystem mUfs;

  private static final int BLOCK_SIZE = 1024 * 1024;

  @Before
  public void before() throws IOException {
    mHdfsConfiguration.set("dfs.name.dir", mTemp.newFolder("nn").getAbsolutePath());
    mHdfsConfiguration.set("dfs.data.dir", mTemp.newFolder("dn").getAbsolutePath());
    // 1MB block size for testing to save memory
    mHdfsConfiguration.setInt("dfs.block.size", BLOCK_SIZE);

    mCluster = new MiniDFSCluster.Builder(mHdfsConfiguration)
        .enableManagedDfsDirsRedundancy(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .numDataNodes(1).build();

    UnderFileSystemConfiguration ufsConf =
        UnderFileSystemConfiguration.defaults(alluxio.conf.Configuration.global());

    setConfiguration();

    mUfs = createUfs(ufsConf);
  }

  @After
  public void after() {
    if (mCluster != null) {
      mCluster.close();
    }
    if (mUfs != null) {
      mUfs.close();
    }
  }

  protected void writeMultiBlockFileTest(String testFilePath) throws IOException {
    // 16MB + 1 byte, 17 blocks
    int fileLength = 1024 * 1024 * 16 + 1;
    int numHdfsBlocks = (fileLength - 1) / BLOCK_SIZE + 1;

    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder()
            .withinRange('0', 'z')
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build();
    String fileContentToWrite = randomStringGenerator.generate(fileLength);

    OutputStream os = mUfs.create(testFilePath, getCreateOption());
    os.write(fileContentToWrite.getBytes());
    os.close();

    InputStream is = mUfs.open(testFilePath);
    String readFileContent = IOUtils.toString(is);
    Assert.assertEquals(fileContentToWrite, readFileContent);

    assertEquals(fileLength, mUfs.getStatus(testFilePath).asUfsFileStatus().getContentLength());
    FileStatus status = mUfs.getFs().getFileStatus(new Path(testFilePath));
    assertEquals(numHdfsBlocks,
        mUfs.getFs().getFileBlockLocations(status, 0, status.getLen()).length);
  }

  protected void writeEmptyFileTest() throws IOException {
    String testFilePath = "/empty_file";
    OutputStream os = mUfs.create(testFilePath, getCreateOption());
    os.close();
    assertEquals(0, mUfs.getStatus(testFilePath).asUfsFileStatus().getContentLength());
  }

  protected void hdfsDownDuringUploadTest() throws Exception {
    String testFilePath = "/test_file";

    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder()
            .withinRange('0', 'z')
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build();
    String oneBlockFileContent = randomStringGenerator.generate(1024 * 1024);

    OutputStream os = mUfs.create(testFilePath, getCreateOption());
    os.write(oneBlockFileContent.getBytes());
    os.write(oneBlockFileContent.getBytes());
    // Stop the data node in the middle of the write process.
    mCluster.stopDataNode(0);
    os.write(oneBlockFileContent.getBytes());
    os.close();
  }

  protected HdfsUnderFileSystem createUfs(UnderFileSystemConfiguration ufsConf) {
    return new HdfsUnderFileSystem(new AlluxioURI("/"), ufsConf, mHdfsConfiguration) {
      @Override
      public FileSystem getFs() throws IOException {
        // Hookup HDFS mini cluster to HDFS UFS
        return mCluster.getFileSystem();
      }
    };
  }

  protected void setConfiguration() {}

  protected CreateOptions getCreateOption() {
    return CreateOptions.defaults(alluxio.conf.Configuration.global());
  }
}

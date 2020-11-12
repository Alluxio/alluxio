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

package alluxio.underfs.cosn;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.DeleteOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;

import java.lang.Boolean;

public class CosnUnderFileSystemTest {
  private CosnUnderFileSystem mCosnFileSystem;
  private String mCosnPath = "cosn://bucket_name/test";
  private CosFileSystem mClient;
  private String mTestFile = "test_file";
  private String mTestPath = "test_path";
  private FileStatus mFileStatus;
  private FileStatus mDirStatus;

  @Before
  public void before() throws Exception {
    mClient = Mockito.mock(CosFileSystem.class);
    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration
                                  .defaults(ConfigurationTestUtils.defaults());
    mCosnFileSystem = new CosnUnderFileSystem(new AlluxioURI(mCosnPath), mClient, conf);
    mFileStatus = new FileStatus(0, false, 3, 0, 0, new Path("/"));
    mDirStatus = new FileStatus(0, true, 3, 0, 0, new Path("/"));
  }

  @Test
  public void deleteFile() throws Exception {
    Mockito.when(mClient.delete(Matchers.any(Path.class), Mockito.anyBoolean())).thenReturn(true);
    Mockito.when(mClient.getFileStatus(Matchers.any(Path.class))).thenReturn(mFileStatus);
    if (mCosnFileSystem.isFile(mTestFile)) {
      boolean ret = mCosnFileSystem.deleteFile(mTestFile);
      Assert.assertTrue(ret);
    }
  }

  @Test
  public void renameFile() throws Exception {
    String srcTestFile = "cosn_test_src.txt";
    String dstTestFile = "cosn_test_dst.txt";
    Mockito.when(mClient.getFileStatus(Matchers.any(Path.class)))
           .thenReturn(mFileStatus);
    Mockito.when(mClient.rename(Matchers.any(Path.class),
            Matchers.any(Path.class)))
            .thenReturn(true);
    boolean ret = mCosnFileSystem.renameFile(srcTestFile, dstTestFile);
    Assert.assertTrue(ret);
  }

  @Test
  public void mkdirAndDeleteDir() throws Exception {
    Mockito.when(mClient.mkdirs(Matchers.any(Path.class),
            Matchers.any(FsPermission.class)))
            .thenReturn(true);
    Mockito.when(mClient.delete(Matchers.any(Path.class),
            Matchers.any(Boolean.class)))
            .thenReturn(true);
    Mockito.when(mClient.getFileStatus(Matchers.any(Path.class))).thenReturn(mDirStatus);

    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration
                      .defaults(ConfigurationTestUtils.defaults());
    MkdirsOptions mkdirOptions = MkdirsOptions.defaults(conf);
    boolean ret = mCosnFileSystem.mkdirs(mTestPath, mkdirOptions);
    Assert.assertTrue(ret);

    DeleteOptions delteOptions = DeleteOptions.defaults();
    ret = mCosnFileSystem.deleteDirectory(mTestPath, delteOptions);
    Assert.assertTrue(ret);
  }
}

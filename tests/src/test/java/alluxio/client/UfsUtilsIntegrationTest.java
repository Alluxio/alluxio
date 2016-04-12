/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystem;
import alluxio.collections.PrefixList;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

/**
 * To test the utilities related to under filesystem, including loadufs and etc.
 */
public class UfsUtilsIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private FileSystem mFileSystem = null;
  private String mUfsRoot = null;
  private UnderFileSystem mUfs = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();

    Configuration masterConf = mLocalAlluxioClusterResource.get().getMasterConf();
    mUfsRoot = masterConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(mUfsRoot + AlluxioURI.SEPARATOR, masterConf);
  }

  @Test
  public void loadUnderFsTest() throws Exception {
    String[] exclusions = {"/alluxio", "/exclusions"};
    String[] inclusions = {"/inclusions/sub-1", "/inclusions/sub-2"};
    for (String exclusion : exclusions) {
      String path = PathUtils.concatPath(mUfsRoot, exclusion);
      if (!mUfs.exists(path)) {
        mUfs.mkdirs(path, true);
      }
    }

    for (String inclusion : inclusions) {
      String path = PathUtils.concatPath(mUfsRoot, inclusion);
      if (!mUfs.exists(path)) {
        mUfs.mkdirs(path, true);
      }
      UnderFileSystemUtils.touch(mUfsRoot + inclusion + "/1",
          mLocalAlluxioClusterResource.get().getMasterConf());
    }

    UfsUtils.loadUfs(new AlluxioURI(AlluxioURI.SEPARATOR), new AlluxioURI(
        mUfsRoot + AlluxioURI.SEPARATOR), new PrefixList("alluxio;exclusions", ";"),
        mLocalAlluxioClusterResource.get().getMasterConf());

    List<String> paths;
    for (String exclusion : exclusions) {
      paths = FileSystemTestUtils.listFiles(mFileSystem, exclusion);
      Assert.assertEquals(0, paths.size());
    }
    for (String inclusion : inclusions) {
      paths = FileSystemTestUtils.listFiles(mFileSystem, inclusion);
      Assert.assertNotEquals(0, paths.size());
    }
  }
}

/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.collections.PrefixList;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.UnderFileSystemUtils;

/**
 * To test the utilities related to under filesystem, including loadufs and etc.
 */
public class UfsUtilsIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(10000, 1000, 128);
  private TachyonFS mTfs = null;
  private TachyonFileSystem mTachyonFileSystem = null;
  private String mUfsRoot = null;
  private UnderFileSystem mUfs = null;

  @Before
  public final void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getOldClient();
    mTachyonFileSystem = mLocalTachyonClusterResource.get().getClient();

    TachyonConf masterConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mUfsRoot = masterConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(mUfsRoot + TachyonURI.SEPARATOR, masterConf);
  }

  @Test
  public void loadUnderFsTest() throws IOException, TException {
    String[] exclusions = {"/tachyon", "/exclusions"};
    String[] inclusions = {"/inclusions/sub-1", "/inclusions/sub-2"};
    for (String exclusion : exclusions) {
      if (!mUfs.exists(exclusion)) {
        mUfs.mkdirs(mUfsRoot + exclusion, true);
      }
    }

    for (String inclusion : inclusions) {
      if (!mUfs.exists(inclusion)) {
        mUfs.mkdirs(mUfsRoot + inclusion, true);
      }
      UnderFileSystemUtils.touch(mUfsRoot + inclusion + "/1",
          mLocalTachyonClusterResource.get().getMasterTachyonConf());
    }

    UfsUtils.loadUfs(mTfs, new TachyonURI(TachyonURI.SEPARATOR), new TachyonURI(mUfsRoot
        + TachyonURI.SEPARATOR), new PrefixList("tachyon;exclusions", ";"),
        mLocalTachyonClusterResource.get().getMasterTachyonConf());

    List<String> paths;
    for (String exclusion : exclusions) {
      paths = TachyonFSTestUtils.listFiles(mTachyonFileSystem, exclusion);
      Assert.assertEquals(0, paths.size());
    }
    for (String inclusion : inclusions) {
      paths = TachyonFSTestUtils.listFiles(mTachyonFileSystem, inclusion);
      Assert.assertNotEquals(0, paths.size());
    }
  }
}

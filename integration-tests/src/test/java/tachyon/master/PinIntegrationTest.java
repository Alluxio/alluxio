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

package tachyon.master;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

public class PinIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private TachyonFS mTfs = null;

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Test
  public void recursivePinness() throws Exception {
    int dir0Id = mTfs.getFileId(new TachyonURI("/"));
    TachyonURI folder = new TachyonURI("/myFolder");
    TachyonURI file = new TachyonURI("/myFolder/myFile");

    mTfs.mkdir(folder);
    int dir1Id = mTfs.getFileId(folder);

    int fileId = mTfs.createFile(file);
    Assert.assertFalse(mTfs.getFile(fileId).needPin());

    mTfs.pinFile(fileId);
    Assert.assertTrue(mTfs.getFile(file).needPin());
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    mTfs.unpinFile(fileId);
    Assert.assertFalse(mTfs.getFile(file).needPin());
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer>newHashSet());

    // Pinning a folder should recursively pin subfolders.
    mTfs.pinFile(dir1Id);
    Assert.assertTrue(mTfs.getFile(file).needPin());
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));

    // Same with unpinning.
    mTfs.unpinFile(dir0Id);
    Assert.assertFalse(mTfs.getFile(file).needPin());
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.<Integer>newHashSet());

    // The last pin command always wins.
    mTfs.pinFile(fileId);
    Assert.assertTrue(mTfs.getFile(file).needPin());
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(fileId));
  }

  @Test
  public void newFilesInheritPinness() throws Exception {
    // Children should inherit the isPinned value of their parents on creation.

    // Pin root
    int rootId = mTfs.getFileId(new TachyonURI("/"));
    mTfs.pinFile(rootId);

    // Child file should be pinned
    int file0Id = mTfs.createFile(new TachyonURI("/file0"));
    Assert.assertTrue(mMasterInfo.getClientFileInfo(file0Id).isPinned);
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // Child folder should be pinned
    mTfs.mkdir(new TachyonURI("/folder"));
    int folderId = mTfs.getFileId(new TachyonURI("/folder"));
    Assert.assertTrue(mMasterInfo.getClientFileInfo(folderId).isPinned);

    // Granchild file also pinned
    int file1Id = mTfs.createFile(new TachyonURI("/folder/file1"));
    Assert.assertTrue(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()),
        Sets.newHashSet(file0Id, file1Id));

    // Unpinning child folder should cause its children to be unpinned as well
    mTfs.unpinFile(folderId);
    Assert.assertFalse(mMasterInfo.getClientFileInfo(folderId).isPinned);
    Assert.assertFalse(mMasterInfo.getClientFileInfo(file1Id).isPinned);
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // And new grandchildren should be unpinned too.
    int file2Id = mTfs.createFile(new TachyonURI("/folder/file2"));
    Assert.assertFalse(mMasterInfo.getClientFileInfo(file2Id).isPinned);
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()), Sets.newHashSet(file0Id));

    // But toplevel children still should be pinned!
    int file3Id = mTfs.createFile(new TachyonURI("/file3"));
    Assert.assertTrue(mMasterInfo.getClientFileInfo(file3Id).isPinned);
    Assert.assertEquals(Sets.newHashSet(mMasterInfo.getPinIdList()),
        Sets.newHashSet(file0Id, file3Id));
  }
}

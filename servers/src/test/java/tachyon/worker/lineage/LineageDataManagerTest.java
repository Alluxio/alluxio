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

package tachyon.worker.lineage;

import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.io.BlockReader;

/**
 * Tests {@link LineageDataManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockDataManager.class, BufferUtils.class})
public final class LineageDataManagerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void persistFileTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    // mock block data manager
    BlockDataManager blockDataManager = Mockito.mock(BlockDataManager.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.path = "test";
    Mockito.when(blockDataManager.getFileInfo(fileId)).thenReturn(fileInfo);
    BlockReader reader = Mockito.mock(BlockReader.class);
    for (long blockId : blockIds) {
      Mockito.when(blockDataManager.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito
          .when(blockDataManager.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId))
          .thenReturn(reader);
    }

    LineageDataManager manager = new LineageDataManager(blockDataManager);

    // mock ufs
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsRoot = new TachyonConf().get(Constants.UNDERFS_ADDRESS);
    Mockito.when(ufs.exists(ufsRoot)).thenReturn(true);
    Whitebox.setInternalState(manager, "mUfs", ufs);
    OutputStream outputStream = Mockito.mock(OutputStream.class);

    // mock BufferUtils
    PowerMockito.mockStatic(BufferUtils.class);

    String dstPath = PathUtils.concatPath(ufsRoot, fileInfo.getPath());
    Mockito.when(ufs.create(dstPath)).thenReturn(outputStream);

    manager.persistFile(fileId, blockIds);

    // verify file persisted
    List<Long> persistedFiles = (List<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertEquals(Lists.newArrayList(fileId), persistedFiles);

    // verify fastCopy called twice, once per block
    PowerMockito.verifyStatic(Mockito.times(2));
    BufferUtils.fastCopy(Mockito.any(ReadableByteChannel.class),
        Mockito.any(WritableByteChannel.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void popPersistedFilesTest() {
    BlockDataManager blockDataManager = Mockito.mock(BlockDataManager.class);
    LineageDataManager manager = new LineageDataManager(blockDataManager);
    List<Long> persistedFiles = Lists.newArrayList(1L, 2L);

    Whitebox.setInternalState(manager, "mPersistedFiles", Lists.newArrayList(persistedFiles));
    List<Long> poppedList = manager.popPersistedFiles();
    Assert.assertEquals(persistedFiles, poppedList);
    // verify persisted files cleared in the manager
    persistedFiles = (List<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertTrue(persistedFiles.isEmpty());
  }

}

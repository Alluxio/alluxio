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

package tachyon.worker.file;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.io.BlockReader;

/**
 * Tests {@link FileDataManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockDataManager.class, BufferUtils.class})
public final class FileDataManagerTest {

  /**
   * Tests that a file gets persisted.
   *
   * @throws Exception when the Whitebox fails
   */
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

    FileDataManager manager = new FileDataManager(blockDataManager);

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
    Set<Long> persistedFiles = (Set<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertEquals(Sets.newHashSet(fileId), persistedFiles);

    // verify fastCopy called twice, once per block
    PowerMockito.verifyStatic(Mockito.times(2));
    BufferUtils.fastCopy(Mockito.any(ReadableByteChannel.class),
        Mockito.any(WritableByteChannel.class));

    // verify the file is not needed for another persistence
    Assert.assertFalse(manager.needPersistence(fileId));
  }

  /**
   * Tests that persisted file are cleared in the manager.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void clearPersistedFilesTest() {
    BlockDataManager blockDataManager = Mockito.mock(BlockDataManager.class);
    FileDataManager manager = new FileDataManager(blockDataManager);
    Set<Long> persistedFiles = Sets.newHashSet(1L, 2L);

    Whitebox.setInternalState(manager, "mPersistedFiles", Sets.newHashSet(persistedFiles));
    List<Long> poppedList = manager.getPersistedFiles();
    Assert.assertEquals(persistedFiles, Sets.newHashSet(poppedList));

    // verify persisted files cleared in the manager
    poppedList.remove(2L);
    manager.clearPersistedFiles(poppedList);
    persistedFiles = (Set<Long>) Whitebox.getInternalState(manager, "mPersistedFiles");
    Assert.assertEquals(Sets.newHashSet(2L), persistedFiles);
  }

  /**
   * Tests that the correct error message is provided when persisting a file fails.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void errorHandlingTest() throws Exception {
    long fileId = 1;
    List<Long> blockIds = Lists.newArrayList(1L, 2L);

    // mock block data manager
    BlockDataManager blockDataManager = Mockito.mock(BlockDataManager.class);
    FileInfo fileInfo = new FileInfo();
    fileInfo.path = "test";
    Mockito.when(blockDataManager.getFileInfo(fileId)).thenReturn(fileInfo);
    for (long blockId : blockIds) {
      Mockito.when(blockDataManager.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId))
          .thenReturn(blockId);
      Mockito.doThrow(new InvalidWorkerStateException("invalid worker")).when(blockDataManager)
          .readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, blockId);
    }

    FileDataManager manager = new FileDataManager(blockDataManager);

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

    try {
      manager.persistFile(fileId, blockIds);
      Assert.fail("the persist should fail");
    } catch (IOException e) {
      Assert.assertEquals("the blocks of file1 are failed to persist\n"
          + "tachyon.exception.InvalidWorkerStateException: invalid worker\n", e.getMessage());
      // verify the locks are all unlocked
      Mockito.verify(blockDataManager).unlockBlock(1L);
      Mockito.verify(blockDataManager).unlockBlock(2L);
    }
  }
}

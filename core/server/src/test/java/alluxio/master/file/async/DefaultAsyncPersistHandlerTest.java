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

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.thrift.PersistFile;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link DefaultAsyncPersistHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public class DefaultAsyncPersistHandlerTest {
  private FileSystemMaster mFileSystemMaster;

  @Before
  public void before() {
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
  }

  @Test
  public void scheduleAsyncPersistTest() throws Exception {
    DefaultAsyncPersistHandler handler =
        new DefaultAsyncPersistHandler(new FileSystemMasterView(mFileSystemMaster));
    AlluxioURI path = new AlluxioURI("/test");
    long blockId = 0;
    long workerId = 1;
    long fileId = 2;
    List<FileBlockInfo> blockInfoList = new ArrayList<>();
    BlockLocation location = new BlockLocation().setWorkerId(workerId);
    blockInfoList.add(new FileBlockInfo().setBlockInfo(
        new BlockInfo().setBlockId(blockId).setLocations(Lists.newArrayList(location))));
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(path)).thenReturn(blockInfoList);
    Mockito.when(mFileSystemMaster.getFileId(path)).thenReturn(fileId);
    Mockito.when(mFileSystemMaster.getPath(fileId)).thenReturn(path);
    Mockito.when(mFileSystemMaster.getFileInfo(fileId))
        .thenReturn(new FileInfo().setCompleted(true));

    handler.scheduleAsyncPersistence(path);
    List<PersistFile> persistFiles = handler.pollFilesToPersist(workerId);
    Assert.assertEquals(1, persistFiles.size());
    Assert.assertEquals(Lists.newArrayList(blockId), persistFiles.get(0).getBlockIds());
  }

  /**
   * Tests the persistence of file with block on multiple workers.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void persistenceFileWithBlocksOnMultipleWorkers() throws Exception {
    DefaultAsyncPersistHandler handler =
        new DefaultAsyncPersistHandler(new FileSystemMasterView(mFileSystemMaster));
    AlluxioURI path = new AlluxioURI("/test");
    List<FileBlockInfo> blockInfoList = new ArrayList<>();
    BlockLocation location1 = new BlockLocation().setWorkerId(1);
    blockInfoList.add(new FileBlockInfo()
        .setBlockInfo(new BlockInfo().setLocations(Lists.newArrayList(location1))));
    BlockLocation location2 = new BlockLocation().setWorkerId(2);
    blockInfoList.add(new FileBlockInfo()
        .setBlockInfo(new BlockInfo().setLocations(Lists.newArrayList(location2))));
    Mockito.when(mFileSystemMaster.getFileBlockInfoList(path)).thenReturn(blockInfoList);

    try {
      handler.scheduleAsyncPersistence(path);
      Assert.fail("Cannot persist with file's blocks distributed on multiple workers");
    } catch (AlluxioException e) {
      Assert.assertEquals("No worker found to schedule async persistence for file /test",
          e.getMessage());
    }
  }
}

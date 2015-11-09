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

package tachyon.client.file;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.FileSystemMasterClient;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidPathException;
import tachyon.thrift.FileInfo;

/**
 * Unit test for functionality in {@link AbstractTachyonFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, ClientContext.class})
public final class AbstractTachyonFileSystemTest {

  private AbstractTachyonFileSystem mFileSystem;
  private FileSystemContext mFileContext;
  private FileSystemMasterClient mFileSystemMasterClient;

  private class DummyTachyonFileSystem extends AbstractTachyonFileSystem {
  }

  @Before
  public void before() {
    ClientContext.reset();
    mFileSystem = new DummyTachyonFileSystem();
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    Whitebox.setInternalState(mFileSystem, "mContext", mFileContext);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(mFileContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
  }

  @After
  public void after() {
    Mockito.verify(mFileContext, Mockito.times(1)).acquireMasterClient();
    Mockito.verify(mFileContext, Mockito.times(1)).releaseMasterClient(mFileSystemMasterClient);
  }

  @Test
  public void createTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.create(Mockito.anyString(), Mockito.<CreateOptions>any()))
        .thenReturn(1L);
    CreateOptions createOptions = CreateOptions.defaults();
    TachyonFile file = mFileSystem.create(new TachyonURI("/"), createOptions);
    Assert.assertEquals(1, file.getFileId());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).create("/", createOptions);
  }

  @Test
  public void deleteTest() throws Exception {
    DeleteOptions deleteOptions = new DeleteOptions.Builder().setRecursive(true).build();
    mFileSystem.delete(new TachyonFile(3), deleteOptions);
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).delete(3L, true);
  }

  @Test
  public void freeTest() throws Exception {
    FreeOptions freeOptions = new FreeOptions.Builder().setRecursive(true).build();
    mFileSystem.free(new TachyonFile(3), freeOptions);
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).free(3L, true);
  }

  @Test
  public void getInfoTest() throws Exception {
    FileInfo info = new FileInfo();
    Mockito.when(mFileSystemMasterClient.getFileInfo(3L)).thenReturn(info);
    GetInfoOptions getInfoOptions = GetInfoOptions.defaults();
    Assert.assertSame(info, mFileSystem.getInfo(new TachyonFile(3), getInfoOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).getFileInfo(3L);
  }

  @Test
  public void listStatusTest() throws Exception {
    List<FileInfo> infos = Lists.newArrayList(new FileInfo());
    Mockito.when(mFileSystemMasterClient.getFileInfoList(3L)).thenReturn(infos);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    Assert.assertSame(infos, mFileSystem.listStatus(new TachyonFile(3), listStatusOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).getFileInfoList(3L);
  }

  @Test
  public void loadMetadataTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.loadMetadata("/", true)).thenReturn(3L);
    LoadMetadataOptions loadMetadataOptions =
        new LoadMetadataOptions.Builder().setRecursive(true).build();
    Assert.assertEquals(3L,
        mFileSystem.loadMetadata(new TachyonURI("/"), loadMetadataOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).loadMetadata("/", true);
  }

  @Test
  public void mkdirTest() throws Exception {
    MkdirOptions mkdirOptions = MkdirOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mkdir("/", mkdirOptions)).thenReturn(false);
    Assert.assertFalse(mFileSystem.mkdir(new TachyonURI("/"), mkdirOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).mkdir("/", mkdirOptions);
  }

  @Test
  public void mountTest() throws Exception {
    TachyonURI tachyonPath = new TachyonURI("/t");
    TachyonURI ufsPath = new TachyonURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mount(tachyonPath, ufsPath)).thenReturn(false);
    Assert.assertFalse(mFileSystem.mount(tachyonPath, ufsPath, mountOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).mount(tachyonPath, ufsPath);
  }

  @Test
  public void openSuccessTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(3L);
    Assert.assertEquals(3L, mFileSystem.open(new TachyonURI("/"), openOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).getFileId("/");
  }

  @Test
  public void openFailTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(-1L);
    try {
      mFileSystem.open(new TachyonURI("/"), openOptions);
      Assert.fail("open should throw InvalidPathException if the file doesn't exist");;
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/"), e.getMessage());
    }
  }

  @Test
  public void openIfExistsSucessTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(3L);
    Assert.assertEquals(3L, mFileSystem.openIfExists(new TachyonURI("/"), openOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).getFileId("/");
  }

  @Test
  public void openIfExistsFailTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(-1L);
    Assert.assertNull(mFileSystem.openIfExists(new TachyonURI("/"), openOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).getFileId("/");
  }

  @Test
  public void renameTest() throws Exception {
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.when(mFileSystemMasterClient.rename(3L, "/")).thenReturn(false);
    Assert.assertFalse(mFileSystem.rename(new TachyonFile(3), new TachyonURI("/"), renameOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).rename(3L, "/");
  }

  @Test
  public void setStateTest() throws Exception {
    SetStateOptions setStateOptions = SetStateOptions.defaults();
    mFileSystem.setState(new TachyonFile(3L), setStateOptions);
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).setState(3L, setStateOptions);
  }

  @Test
  public void unmountTest() throws Exception {
    TachyonURI path = new TachyonURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.unmount(path)).thenReturn(false);
    Assert.assertFalse(mFileSystem.unmount(path, unmountOptions));
    Mockito.verify(mFileSystemMasterClient, Mockito.times(1)).unmount(path);
  }
}

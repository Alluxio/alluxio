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

  private static final long FILE_ID = 3L;
  private static final long NO_FILE_CODE = -1L;
  private static final RuntimeException EXCEPTION = new RuntimeException("test exception");
  private static final String SHOULD_HAVE_PROPAGATED_MESSAGE =
      "Exception should have been propagated";

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
    Mockito.verify(mFileContext).acquireMasterClient();
    Mockito.verify(mFileContext).releaseMasterClient(mFileSystemMasterClient);
  }

  @Test
  public void createTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.create(Mockito.anyString(), Mockito.<CreateOptions>any()))
        .thenReturn(FILE_ID);
    CreateOptions createOptions = CreateOptions.defaults();
    TachyonFile file = mFileSystem.create(new TachyonURI("/"), createOptions);
    Assert.assertEquals(FILE_ID, file.getFileId());
    Mockito.verify(mFileSystemMasterClient).create("/", createOptions);
  }

  @Test
  public void createExceptionTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.create(Mockito.anyString(), Mockito.<CreateOptions>any()))
        .thenThrow(EXCEPTION);
    CreateOptions createOptions = CreateOptions.defaults();
    try {
      mFileSystem.create(new TachyonURI("/"), createOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void deleteTest() throws Exception {
    DeleteOptions deleteOptions = new DeleteOptions.Builder().setRecursive(true).build();
    mFileSystem.delete(new TachyonFile(FILE_ID), deleteOptions);
    Mockito.verify(mFileSystemMasterClient).delete(FILE_ID, true);
  }

  @Test
  public void deleteExceptionTest() throws Exception {
    DeleteOptions deleteOptions = new DeleteOptions.Builder().setRecursive(true).build();
    Mockito.when(mFileSystemMasterClient.delete(FILE_ID, true)).thenThrow(EXCEPTION);
    try {
      mFileSystem.delete(new TachyonFile(FILE_ID), deleteOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void freeTest() throws Exception {
    FreeOptions freeOptions = new FreeOptions.Builder().setRecursive(true).build();
    mFileSystem.free(new TachyonFile(FILE_ID), freeOptions);
    Mockito.verify(mFileSystemMasterClient).free(FILE_ID, true);
  }

  @Test
  public void freeExceptionTest() throws Exception {
    FreeOptions freeOptions = new FreeOptions.Builder().setRecursive(true).build();
    Mockito.when(mFileSystemMasterClient.free(FILE_ID, true)).thenThrow(EXCEPTION);
    try {
      mFileSystem.free(new TachyonFile(FILE_ID), freeOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void getInfoTest() throws Exception {
    FileInfo info = new FileInfo();
    Mockito.when(mFileSystemMasterClient.getFileInfo(FILE_ID)).thenReturn(info);
    GetInfoOptions getInfoOptions = GetInfoOptions.defaults();
    Assert.assertSame(info, mFileSystem.getInfo(new TachyonFile(FILE_ID), getInfoOptions));
    Mockito.verify(mFileSystemMasterClient).getFileInfo(FILE_ID);
  }

  @Test
  public void getInfoExceptionTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.getFileInfo(FILE_ID)).thenThrow(EXCEPTION);
    GetInfoOptions getInfoOptions = GetInfoOptions.defaults();
    try {
      mFileSystem.getInfo(new TachyonFile(FILE_ID), getInfoOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void listStatusTest() throws Exception {
    List<FileInfo> infos = Lists.newArrayList(new FileInfo());
    Mockito.when(mFileSystemMasterClient.getFileInfoList(FILE_ID)).thenReturn(infos);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    Assert.assertSame(infos, mFileSystem.listStatus(new TachyonFile(FILE_ID), listStatusOptions));
    Mockito.verify(mFileSystemMasterClient).getFileInfoList(FILE_ID);
  }

  @Test
  public void listStatusExceptionTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.getFileInfoList(FILE_ID)).thenThrow(EXCEPTION);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    try {
      mFileSystem.listStatus(new TachyonFile(FILE_ID), listStatusOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void loadMetadataTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.loadMetadata("/", true)).thenReturn(FILE_ID);
    LoadMetadataOptions loadMetadataOptions =
        new LoadMetadataOptions.Builder().setRecursive(true).build();
    Assert.assertEquals(FILE_ID,
        mFileSystem.loadMetadata(new TachyonURI("/"), loadMetadataOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient).loadMetadata("/", true);
  }

  @Test
  public void loadMetadataExceptionTest() throws Exception {
    Mockito.when(mFileSystemMasterClient.loadMetadata("/", true)).thenThrow(EXCEPTION);
    LoadMetadataOptions loadMetadataOptions =
        new LoadMetadataOptions.Builder().setRecursive(true).build();
    try {
      mFileSystem.loadMetadata(new TachyonURI("/"), loadMetadataOptions).getFileId();
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void mkdirTest() throws Exception {
    MkdirOptions mkdirOptions = MkdirOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mkdir("/", mkdirOptions)).thenReturn(false);
    Assert.assertFalse(mFileSystem.mkdir(new TachyonURI("/"), mkdirOptions));
    Mockito.verify(mFileSystemMasterClient).mkdir("/", mkdirOptions);
  }

  @Test
  public void mkdirExceptionTest() throws Exception {
    MkdirOptions mkdirOptions = MkdirOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mkdir("/", mkdirOptions)).thenThrow(EXCEPTION);
    try {
      mFileSystem.mkdir(new TachyonURI("/"), mkdirOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void mountTest() throws Exception {
    TachyonURI tachyonPath = new TachyonURI("/t");
    TachyonURI ufsPath = new TachyonURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mount(tachyonPath, ufsPath)).thenReturn(false);
    Assert.assertFalse(mFileSystem.mount(tachyonPath, ufsPath, mountOptions));
    Mockito.verify(mFileSystemMasterClient).mount(tachyonPath, ufsPath);
  }

  @Test
  public void mountExceptionTest() throws Exception {
    TachyonURI tachyonPath = new TachyonURI("/t");
    TachyonURI ufsPath = new TachyonURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.mount(tachyonPath, ufsPath)).thenThrow(EXCEPTION);
    try {
      mFileSystem.mount(tachyonPath, ufsPath, mountOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void openSuccessTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(FILE_ID);
    Assert.assertEquals(FILE_ID, mFileSystem.open(new TachyonURI("/"), openOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient).getFileId("/");
  }

  @Test
  public void openFailTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(NO_FILE_CODE);
    try {
      mFileSystem.open(new TachyonURI("/"), openOptions);
      Assert.fail("open should throw InvalidPathException if the file doesn't exist");;
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/"), e.getMessage());
    }
  }

  @Test
  public void openExceptionTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenThrow(EXCEPTION);
    try {
      mFileSystem.open(new TachyonURI("/"), openOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void openIfExistsSucessTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(FILE_ID);
    Assert.assertEquals(FILE_ID,
        mFileSystem.openIfExists(new TachyonURI("/"), openOptions).getFileId());
    Mockito.verify(mFileSystemMasterClient).getFileId("/");
  }

  @Test
  public void openIfExistsFailTest() throws Exception {
    OpenOptions openOptions = OpenOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getFileId("/")).thenReturn(NO_FILE_CODE);
    Assert.assertNull(mFileSystem.openIfExists(new TachyonURI("/"), openOptions));
    Mockito.verify(mFileSystemMasterClient).getFileId("/");
  }

  @Test
  public void renameTest() throws Exception {
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.when(mFileSystemMasterClient.rename(FILE_ID, "/")).thenReturn(false);
    Assert.assertFalse(
        mFileSystem.rename(new TachyonFile(FILE_ID), new TachyonURI("/"), renameOptions));
    Mockito.verify(mFileSystemMasterClient).rename(FILE_ID, "/");
  }

  @Test
  public void renameExceptionTest() throws Exception {
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.when(mFileSystemMasterClient.rename(FILE_ID, "/")).thenThrow(EXCEPTION);
    try {
      mFileSystem.rename(new TachyonFile(FILE_ID), new TachyonURI("/"), renameOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void setStateTest() throws Exception {
    SetStateOptions setStateOptions = SetStateOptions.defaults();
    mFileSystem.setState(new TachyonFile(FILE_ID), setStateOptions);
    Mockito.verify(mFileSystemMasterClient).setState(FILE_ID, setStateOptions);
  }

  @Test
  public void setStateExceptionTest() throws Exception {
    SetStateOptions setStateOptions = SetStateOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).setState(FILE_ID, setStateOptions);
    try {
      mFileSystem.setState(new TachyonFile(FILE_ID), setStateOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  @Test
  public void unmountTest() throws Exception {
    TachyonURI path = new TachyonURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.unmount(path)).thenReturn(false);
    Assert.assertFalse(mFileSystem.unmount(path, unmountOptions));
    Mockito.verify(mFileSystemMasterClient).unmount(path);
  }

  @Test
  public void unmountExceptionTest() throws Exception {
    TachyonURI path = new TachyonURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.when(mFileSystemMasterClient.unmount(path)).thenThrow(EXCEPTION);
    try {
      mFileSystem.unmount(path, unmountOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }
}

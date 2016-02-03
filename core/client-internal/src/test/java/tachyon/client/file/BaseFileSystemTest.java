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
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetStatusOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.thrift.FileInfo;

/**
* Unit test for functionality in {@link BaseFileSystem}.
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class, ClientContext.class})
public final class BaseFileSystemTest {

  private static final RuntimeException EXCEPTION = new RuntimeException("test exception");
  private static final String SHOULD_HAVE_PROPAGATED_MESSAGE =
      "Exception should have been propagated";

  private FileSystem mFileSystem;
  private FileSystemContext mFileContext;
  private FileSystemMasterClient mFileSystemMasterClient;

  private class DummyTachyonFileSystem extends BaseFileSystem {
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mFileSystem = new DummyTachyonFileSystem();
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    Whitebox.setInternalState(mFileSystem, "mContext", mFileContext);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    Mockito.when(mFileContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
  }

  /**
   * Releases the client after a test ran.
   */
  @After
  public void after() {
    Mockito.verify(mFileContext).acquireMasterClient();
    Mockito.verify(mFileContext).releaseMasterClient(mFileSystemMasterClient);
  }

  /**
   * Tests the creation of a file via the
   * {@link BaseFileSystem#createFile(TachyonURI, CreateFileOptions)} method.
   *
   * @throws Exception when the client or file system cannot be created
   */
  @Test
  public void createFileTest() throws Exception {
    Mockito.doNothing().when(mFileSystemMasterClient)
        .createFile(Mockito.any(TachyonURI.class), Mockito.any(CreateFileOptions.class));
    TachyonURI file = new TachyonURI("/file");
    CreateFileOptions options = CreateFileOptions.defaults();
    FileOutStream out = mFileSystem.createFile(file, options);
    Mockito.verify(mFileSystemMasterClient).createFile(file, options);
    Assert.assertEquals(out.mUri, file);
  }

  /**
   * Ensures that an exception is propagated correctly when creating a file system.
   *
   * @throws Exception when the client or file system cannot be created
   */
  @Test
  public void createExceptionTest() throws Exception {
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createFile(Mockito.any(TachyonURI.class), Mockito.any(CreateFileOptions.class));
    CreateFileOptions options = CreateFileOptions.defaults();
    try {
      mFileSystem.createFile(new TachyonURI("/"), options);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#delete(TachyonURI, DeleteOptions)} method.
   *
   * @throws Exception when the file system cannot delete the file
   */
  @Test
  public void deleteTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    DeleteOptions deleteOptions = DeleteOptions.defaults().setRecursive(true);
    mFileSystem.delete(file, deleteOptions);
    Mockito.verify(mFileSystemMasterClient).delete(file, deleteOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when deleting a file.
   *
   * @throws Exception when the file system cannot delete the file
   */
  @Test
  public void deleteExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    DeleteOptions deleteOptions = DeleteOptions.defaults().setRecursive(true);
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).delete(file, deleteOptions);
    try {
      mFileSystem.delete(file, deleteOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#free(TachyonURI, FreeOptions)} method.
   *
   * @throws Exception when the file system cannot free the file
   */
  @Test
  public void freeTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    FreeOptions freeOptions = FreeOptions.defaults().setRecursive(true);
    mFileSystem.free(file, freeOptions);
    Mockito.verify(mFileSystemMasterClient).free(file, freeOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when freeing a file.
   *
   * @throws Exception when the file system cannot free the file
   */
  @Test
  public void freeExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    FreeOptions freeOptions = FreeOptions.defaults().setRecursive(true);
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).free(file, freeOptions);
    try {
      mFileSystem.free(file, freeOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#getStatus(TachyonURI, GetStatusOptions)} method.
   *
   * @throws Exception when the information cannot be retrieved
   */
  @Test
  public void getStatusTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    Mockito.when(mFileSystemMasterClient.getStatus(file)).thenReturn(status);
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    Assert.assertSame(status, mFileSystem.getStatus(file, getStatusOptions));
    Mockito.verify(mFileSystemMasterClient).getStatus(file);
  }

  /**
   * Ensures that an exception is propagated correctly when retrieving information.
   *
   * @throws Exception when the information cannot be retrieved
   */
  @Test
  public void getStatusExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    Mockito.when(mFileSystemMasterClient.getStatus(file)).thenThrow(EXCEPTION);
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    try {
      mFileSystem.getStatus(file, getStatusOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#listStatus(TachyonURI, ListStatusOptions)}
   * method.
   *
   * @throws Exception when listing the status fails
   */
  @Test
  public void listStatusTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    List<URIStatus> infos = Lists.newArrayList(new URIStatus(new FileInfo()));
    Mockito.when(mFileSystemMasterClient.listStatus(file)).thenReturn(infos);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    Assert.assertSame(infos, mFileSystem.listStatus(file, listStatusOptions));
    Mockito.verify(mFileSystemMasterClient).listStatus(file);
  }

  /**
   * Ensures that an exception is propagated correctly when listing the status.
   *
   * @throws Exception when listing the status fails
   */
  @Test
  public void listStatusExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    Mockito.when(mFileSystemMasterClient.listStatus(file)).thenThrow(EXCEPTION);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    try {
      mFileSystem.listStatus(file, listStatusOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#loadMetadata(TachyonURI, LoadMetadataOptions)}
   * method.
   *
   * @throws Exception when loading the metadata fails
   */
  @Test
  public void loadMetadataTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
    Mockito.doNothing().when(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
    mFileSystem.loadMetadata(file, loadMetadataOptions);
    Mockito.verify(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when loading the metadata.
   *
   * @throws Exception when loading the metadata fails
   */
  @Test
  public void loadMetadataExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .loadMetadata(file, loadMetadataOptions);
    try {
      mFileSystem.loadMetadata(file, loadMetadataOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#createDirectory(TachyonURI, CreateDirectoryOptions)}
   * method.
   *
   * @throws Exception when the creation of the directory fails
   */
  @Test
  public void createDirectoryTest() throws Exception {
    TachyonURI dir = new TachyonURI("/dir");
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);
    mFileSystem.createDirectory(dir, createDirectoryOptions);
    Mockito.verify(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when creating a directory.
   *
   * @throws Exception when the creation of the directory fails
   */
  @Test
  public void createDirectoryExceptionTest() throws Exception {
    TachyonURI dir = new TachyonURI("/dir");
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createDirectory(dir, createDirectoryOptions);
    try {
      mFileSystem.createDirectory(dir, createDirectoryOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#mount(TachyonURI, TachyonURI, MountOptions)} method.
   *
   * @throws Exception when the path cannot be mounted
   */
  @Test
  public void mountTest() throws Exception {
    TachyonURI tachyonPath = new TachyonURI("/t");
    TachyonURI ufsPath = new TachyonURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).mount(tachyonPath, ufsPath);
    mFileSystem.mount(tachyonPath, ufsPath, mountOptions);
    Mockito.verify(mFileSystemMasterClient).mount(tachyonPath, ufsPath);
  }

  /**
   * Ensures that an exception is propagated correctly when mounting a path.
   *
   * @throws Exception when the path cannot be mounted
   */
  @Test
  public void mountExceptionTest() throws Exception {
    TachyonURI tachyonPath = new TachyonURI("/t");
    TachyonURI ufsPath = new TachyonURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).mount(tachyonPath, ufsPath);
    try {
      mFileSystem.mount(tachyonPath, ufsPath, mountOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#openFile(TachyonURI, OpenFileOptions)} method to
   * complete successfully.
   *
   * @throws Exception when opening the file fails
   */
  @Test
  public void openFileTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    Mockito.when(mFileSystemMasterClient.getStatus(file)).thenReturn(status);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    mFileSystem.openFile(file, openOptions);
    Mockito.verify(mFileSystemMasterClient).getStatus(file);
  }

  /**
   * Ensures that an exception is propagated successfully when opening a file.
   *
   * @throws Exception when opening the file fails
   */
  @Test
  public void openExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    Mockito.when(mFileSystemMasterClient.getStatus(file)).thenThrow(EXCEPTION);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    try {
      mFileSystem.openFile(file, openOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#rename(TachyonURI, TachyonURI, RenameOptions)}
   * method.
   *
   * @throws Exception when renaming the file fails
   */
  @Test
  public void renameTest() throws Exception {
    TachyonURI src = new TachyonURI("/file");
    TachyonURI dst = new TachyonURI("/file2");
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).rename(src, dst);
    mFileSystem.rename(src, dst, renameOptions);
    Mockito.verify(mFileSystemMasterClient).rename(src, dst);
  }

  /**
   * Ensures that an exception is propagated successfully when renaming a file.
   *
   * @throws Exception when renaming the file fails
   */
  @Test
  public void renameExceptionTest() throws Exception {
    TachyonURI src = new TachyonURI("/file");
    TachyonURI dst = new TachyonURI("/file2");
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).rename(src, dst);
    try {
      mFileSystem.rename(src, dst, renameOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#setAttribute(TachyonURI, SetAttributeOptions)} method.
   *
   * @throws Exception when setting the state fails
   */
  @Test
  public void setAttributeTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    SetAttributeOptions setAttributeOptions = SetAttributeOptions.defaults();
    mFileSystem.setAttribute(file, setAttributeOptions);
    Mockito.verify(mFileSystemMasterClient).setAttribute(file, setAttributeOptions);
  }

  /**
   * Ensures that an exception is propagated successfully when setting the state.
   *
   * @throws Exception when setting the state fails
   */
  @Test
  public void setStateExceptionTest() throws Exception {
    TachyonURI file = new TachyonURI("/file");
    SetAttributeOptions setAttributeOptions = SetAttributeOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .setAttribute(file, setAttributeOptions);
    try {
      mFileSystem.setAttribute(file, setAttributeOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#unmount(TachyonURI, UnmountOptions)} method.
   *
   * @throws Exception when unmounting the path fails
   */
  @Test
  public void unmountTest() throws Exception {
    TachyonURI path = new TachyonURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).unmount(path);
    mFileSystem.unmount(path, unmountOptions);
    Mockito.verify(mFileSystemMasterClient).unmount(path);
  }

  /**
   * Ensures that an exception is propagated successfully when unmounting a path.
   *
   * @throws Exception when unmounting the path fails
   */
  @Test
  public void unmountExceptionTest() throws Exception {
    TachyonURI path = new TachyonURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient).unmount(path);
    try {
      mFileSystem.unmount(path, unmountOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }
}

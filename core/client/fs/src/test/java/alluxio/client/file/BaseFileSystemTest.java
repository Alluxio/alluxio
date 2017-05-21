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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

/**
* Unit test for functionality in {@link BaseFileSystem}.
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemContext.class, FileSystemMasterClient.class})
public final class BaseFileSystemTest {

  private static final RuntimeException EXCEPTION = new RuntimeException("test exception");
  private static final String SHOULD_HAVE_PROPAGATED_MESSAGE =
      "Exception should have been propagated";

  private FileSystem mFileSystem;
  private FileSystemContext mFileContext;
  private FileSystemMasterClient mFileSystemMasterClient;

  private class DummyAlluxioFileSystem extends BaseFileSystem {
    public DummyAlluxioFileSystem(FileSystemContext context) {
      super(context);
    }
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystem = new DummyAlluxioFileSystem(mFileContext);
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
   * {@link BaseFileSystem#createFile(AlluxioURI, CreateFileOptions)} method.
   */
  @Test
  public void createFile() throws Exception {
    Mockito.doNothing().when(mFileSystemMasterClient)
        .createFile(Mockito.any(AlluxioURI.class), Mockito.any(CreateFileOptions.class));
    URIStatus status = new URIStatus(new FileInfo());
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults().setLoadMetadataType(
        LoadMetadataType.Never);
    Mockito.when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    CreateFileOptions options = CreateFileOptions.defaults();
    FileOutStream out = mFileSystem.createFile(file, options);
    Mockito.verify(mFileSystemMasterClient).createFile(file, options);
    Assert.assertEquals(out.mUri, file);
  }

  /**
   * Ensures that an exception is propagated correctly when creating a file system.
   */
  @Test
  public void createException() throws Exception {
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createFile(Mockito.any(AlluxioURI.class), Mockito.any(CreateFileOptions.class));
    CreateFileOptions options = CreateFileOptions.defaults();
    try {
      mFileSystem.createFile(new AlluxioURI("/"), options);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#delete(AlluxioURI, DeleteOptions)} method.
   */
  @Test
  public void delete() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    DeleteOptions deleteOptions = DeleteOptions.defaults().setRecursive(true);
    mFileSystem.delete(file, deleteOptions);
    Mockito.verify(mFileSystemMasterClient).delete(file, deleteOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when deleting a file.
   */
  @Test
  public void deleteException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
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
   * Tests for the {@link BaseFileSystem#free(AlluxioURI, FreeOptions)} method.
   */
  @Test
  public void free() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    FreeOptions freeOptions = FreeOptions.defaults().setRecursive(true);
    mFileSystem.free(file, freeOptions);
    Mockito.verify(mFileSystemMasterClient).free(file, freeOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when freeing a file.
   */
  @Test
  public void freeException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
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
   * Tests for the {@link BaseFileSystem#getStatus(AlluxioURI, GetStatusOptions)} method.
   */
  @Test
  public void getStatus() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    Assert.assertSame(status, mFileSystem.getStatus(file, getStatusOptions));
    Mockito.verify(mFileSystemMasterClient).getStatus(file, getStatusOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when retrieving information.
   */
  @Test
  public void getStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenThrow(EXCEPTION);
    try {
      mFileSystem.getStatus(file, getStatusOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#listStatus(AlluxioURI, ListStatusOptions)} method.
   */
  @Test
  public void listStatus() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    List<URIStatus> infos = new ArrayList<>();
    infos.add(new URIStatus(new FileInfo()));
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    Mockito.when(mFileSystemMasterClient.listStatus(file, listStatusOptions)).thenReturn(infos);
    Assert.assertSame(infos, mFileSystem.listStatus(file, listStatusOptions));
    Mockito.verify(mFileSystemMasterClient).listStatus(file, listStatusOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when listing the status.
   */
  @Test
  public void listStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    Mockito.when(mFileSystemMasterClient.listStatus(file, ListStatusOptions.defaults()))
        .thenThrow(EXCEPTION);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    try {
      mFileSystem.listStatus(file, listStatusOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#loadMetadata(AlluxioURI, LoadMetadataOptions)}
   * method.
   */
  @Test
  public void loadMetadata() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
    Mockito.doNothing().when(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
    mFileSystem.loadMetadata(file, loadMetadataOptions);
    Mockito.verify(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when loading the metadata.
   */
  @Test
  public void loadMetadataException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
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
   * Tests for the {@link BaseFileSystem#createDirectory(AlluxioURI, CreateDirectoryOptions)}
   * method.
   */
  @Test
  public void createDirectory() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);
    mFileSystem.createDirectory(dir, createDirectoryOptions);
    Mockito.verify(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when creating a directory.
   */
  @Test
  public void createDirectoryException() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
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
   * Tests for the {@link BaseFileSystem#mount(AlluxioURI, AlluxioURI, MountOptions)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).mount(alluxioPath, ufsPath, mountOptions);
    mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
    Mockito.verify(mFileSystemMasterClient).mount(alluxioPath, ufsPath, mountOptions);
  }

  /**
   * Ensures that an exception is propagated correctly when mounting a path.
   */
  @Test
  public void mountException() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    Mockito.doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .mount(alluxioPath, ufsPath, mountOptions);
    try {
      mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#openFile(AlluxioURI, OpenFileOptions)} method to
   * complete successfully.
   */
  @Test
  public void openFile() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    mFileSystem.openFile(file, openOptions);
    Mockito.verify(mFileSystemMasterClient).getStatus(file, getStatusOptions);
  }

  /**
   * Ensures that an exception is propagated successfully when opening a file.
   */
  @Test
  public void openException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    Mockito.when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenThrow(EXCEPTION);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    try {
      mFileSystem.openFile(file, openOptions);
      Assert.fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      Assert.assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#rename(AlluxioURI, AlluxioURI, RenameOptions)}
   * method.
   */
  @Test
  public void rename() throws Exception {
    AlluxioURI src = new AlluxioURI("/file");
    AlluxioURI dst = new AlluxioURI("/file2");
    RenameOptions renameOptions = RenameOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).rename(src, dst);
    mFileSystem.rename(src, dst, renameOptions);
    Mockito.verify(mFileSystemMasterClient).rename(src, dst);
  }

  /**
   * Ensures that an exception is propagated successfully when renaming a file.
   */
  @Test
  public void renameException() throws Exception {
    AlluxioURI src = new AlluxioURI("/file");
    AlluxioURI dst = new AlluxioURI("/file2");
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
   * Tests for the {@link BaseFileSystem#setAttribute(AlluxioURI, SetAttributeOptions)} method.
   */
  @Test
  public void setAttribute() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    SetAttributeOptions setAttributeOptions = SetAttributeOptions.defaults();
    mFileSystem.setAttribute(file, setAttributeOptions);
    Mockito.verify(mFileSystemMasterClient).setAttribute(file, setAttributeOptions);
  }

  /**
   * Ensures that an exception is propagated successfully when setting the state.
   */
  @Test
  public void setStateException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
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
   * Tests for the {@link BaseFileSystem#unmount(AlluxioURI, UnmountOptions)} method.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI path = new AlluxioURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    Mockito.doNothing().when(mFileSystemMasterClient).unmount(path);
    mFileSystem.unmount(path, unmountOptions);
    Mockito.verify(mFileSystemMasterClient).unmount(path);
  }

  /**
   * Ensures that an exception is propagated successfully when unmounting a path.
   */
  @Test
  public void unmountException() throws Exception {
    AlluxioURI path = new AlluxioURI("/");
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

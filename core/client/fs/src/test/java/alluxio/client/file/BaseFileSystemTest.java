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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.TestLoggerRule;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
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

  @Rule
  private TestLoggerRule mTestLogger = new TestLoggerRule();

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
    when(mFileContext.acquireMasterClient()).thenReturn(mFileSystemMasterClient);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Verifies and releases the master client after a test with a filesystem operation.
   */
  public void verifyFilesystemContextAcquiredAndReleased() {
    verify(mFileContext).acquireMasterClient();
    verify(mFileContext).releaseMasterClient(mFileSystemMasterClient);
  }

  /**
   * Tests the creation of a file via the
   * {@link BaseFileSystem#createFile(AlluxioURI, CreateFileOptions)} method.
   */
  @Test
  public void createFile() throws Exception {
    doNothing().when(mFileSystemMasterClient)
        .createFile(any(AlluxioURI.class), any(CreateFileOptions.class));
    URIStatus status = new URIStatus(new FileInfo());
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults().setLoadMetadataType(
        LoadMetadataType.Never);
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    CreateFileOptions options = CreateFileOptions.defaults();
    FileOutStream out = mFileSystem.createFile(file, options);
    verify(mFileSystemMasterClient).createFile(file, options);
    assertEquals(out.mUri, file);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when creating a file system.
   */
  @Test
  public void createException() throws Exception {
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createFile(any(AlluxioURI.class), any(CreateFileOptions.class));
    CreateFileOptions options = CreateFileOptions.defaults();
    try {
      mFileSystem.createFile(new AlluxioURI("/"), options);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#delete(AlluxioURI, DeleteOptions)} method.
   */
  @Test
  public void delete() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    DeleteOptions deleteOptions = DeleteOptions.defaults().setRecursive(true);
    mFileSystem.delete(file, deleteOptions);
    verify(mFileSystemMasterClient).delete(file, deleteOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when deleting a file.
   */
  @Test
  public void deleteException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    DeleteOptions deleteOptions = DeleteOptions.defaults().setRecursive(true);
    doThrow(EXCEPTION).when(mFileSystemMasterClient).delete(file, deleteOptions);
    try {
      mFileSystem.delete(file, deleteOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#free(AlluxioURI, FreeOptions)} method.
   */
  @Test
  public void free() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    FreeOptions freeOptions = FreeOptions.defaults().setRecursive(true);
    mFileSystem.free(file, freeOptions);
    verify(mFileSystemMasterClient).free(file, freeOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when freeing a file.
   */
  @Test
  public void freeException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    FreeOptions freeOptions = FreeOptions.defaults().setRecursive(true);
    doThrow(EXCEPTION).when(mFileSystemMasterClient).free(file, freeOptions);
    try {
      mFileSystem.free(file, freeOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#getStatus(AlluxioURI, GetStatusOptions)} method.
   */
  @Test
  public void getStatus() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    assertSame(status, mFileSystem.getStatus(file, getStatusOptions));
    verify(mFileSystemMasterClient).getStatus(file, getStatusOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when retrieving information.
   */
  @Test
  public void getStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenThrow(EXCEPTION);
    try {
      mFileSystem.getStatus(file, getStatusOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
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
    when(mFileSystemMasterClient.listStatus(file, listStatusOptions)).thenReturn(infos);
    assertSame(infos, mFileSystem.listStatus(file, listStatusOptions));
    verify(mFileSystemMasterClient).listStatus(file, listStatusOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when listing the status.
   */
  @Test
  public void listStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    when(mFileSystemMasterClient.listStatus(file, ListStatusOptions.defaults()))
        .thenThrow(EXCEPTION);
    ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
    try {
      mFileSystem.listStatus(file, listStatusOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#loadMetadata(AlluxioURI, LoadMetadataOptions)}
   * method.
   */
  @Test
  public void loadMetadata() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
    doNothing().when(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
    mFileSystem.loadMetadata(file, loadMetadataOptions);
    verify(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when loading the metadata.
   */
  @Test
  public void loadMetadataException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .loadMetadata(file, loadMetadataOptions);
    try {
      mFileSystem.loadMetadata(file, loadMetadataOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#createDirectory(AlluxioURI, CreateDirectoryOptions)}
   * method.
   */
  @Test
  public void createDirectory() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults();
    doNothing().when(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);
    mFileSystem.createDirectory(dir, createDirectoryOptions);
    verify(mFileSystemMasterClient).createDirectory(dir, createDirectoryOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when creating a directory.
   */
  @Test
  public void createDirectoryException() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults();
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createDirectory(dir, createDirectoryOptions);
    try {
      mFileSystem.createDirectory(dir, createDirectoryOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#mount(AlluxioURI, AlluxioURI, MountOptions)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    doNothing().when(mFileSystemMasterClient).mount(alluxioPath, ufsPath, mountOptions);
    mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
    verify(mFileSystemMasterClient).mount(alluxioPath, ufsPath, mountOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when mounting a path.
   */
  @Test
  public void mountException() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountOptions mountOptions = MountOptions.defaults();
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .mount(alluxioPath, ufsPath, mountOptions);
    try {
      mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
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
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenReturn(status);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    mFileSystem.openFile(file, openOptions);
    verify(mFileSystemMasterClient).getStatus(file, getStatusOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated successfully when opening a file.
   */
  @Test
  public void openException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusOptions getStatusOptions = GetStatusOptions.defaults();
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions)).thenThrow(EXCEPTION);
    OpenFileOptions openOptions = OpenFileOptions.defaults();
    try {
      mFileSystem.openFile(file, openOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
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
    doNothing().when(mFileSystemMasterClient).rename(src, dst, renameOptions);
    mFileSystem.rename(src, dst, renameOptions);
    verify(mFileSystemMasterClient).rename(src, dst, renameOptions);
  }

  /**
   * Ensures that an exception is propagated successfully when renaming a file.
   */
  @Test
  public void renameException() throws Exception {
    AlluxioURI src = new AlluxioURI("/file");
    AlluxioURI dst = new AlluxioURI("/file2");
    RenameOptions renameOptions = RenameOptions.defaults();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).rename(src, dst, renameOptions);
    try {
      mFileSystem.rename(src, dst, renameOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
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
    verify(mFileSystemMasterClient).setAttribute(file, setAttributeOptions);
  }

  /**
   * Ensures that an exception is propagated successfully when setting the state.
   */
  @Test
  public void setStateException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    SetAttributeOptions setAttributeOptions = SetAttributeOptions.defaults();
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .setAttribute(file, setAttributeOptions);
    try {
      mFileSystem.setAttribute(file, setAttributeOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#unmount(AlluxioURI, UnmountOptions)} method.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI path = new AlluxioURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    doNothing().when(mFileSystemMasterClient).unmount(path);
    mFileSystem.unmount(path, unmountOptions);
    verify(mFileSystemMasterClient).unmount(path);
  }

  /**
   * Ensures that an exception is propagated successfully when unmounting a path.
   */
  @Test
  public void unmountException() throws Exception {
    AlluxioURI path = new AlluxioURI("/");
    UnmountOptions unmountOptions = UnmountOptions.defaults();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).unmount(path);
    try {
      mFileSystem.unmount(path, unmountOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }
  }

  /**
   * Ensures warnings are logged and an exception is thrown when an {@link AlluxioURI} with an
   * invalid authority is passed.
   */
  @Test
  public void uriCheckBadAuthority() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");

    AlluxioURI uri = new AlluxioURI("alluxio://localhost:1234/root");
    try {
      mFileSystem.createDirectory(uri);
      fail("Should have failed on bad host and port");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("does not match the configured value of"));
    }

    assertTrue(mTestLogger.mAppender.wasLogged("The URI scheme"));
    assertTrue(mTestLogger.mAppender.wasLogged("The URI authority"));

  }

  /**
   * Ensures an exception is thrown when an invalid scheme is passed.
   */
  @Test
  public void uriCheckBadScheme() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");
    AlluxioURI uri = new AlluxioURI("hdfs://localhost:19998/root");
    try {
      mFileSystem.createDirectory(uri);
      fail("Should have failed on bad host and port");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Scheme hdfs:// in AlluxioURI is invalid"));
    }

  }

  /**
   * Ensures there is one warning when a URI with a valid scheme and authority is passed.
   */
  @Test
  public void uriCheckGoodSchemeAndAuthority() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");

    AlluxioURI uri = new AlluxioURI("alluxio://localhost:19998/root");
    mFileSystem.createDirectory(uri);

    assertTrue(mTestLogger.mAppender.wasLogged("The URI scheme"));
    assertTrue(mTestLogger.mAppender.wasLogged("The URI authority"));

  }

  /**
   * Ensures there is no warnings or errors when an {@link AlluxioURI} without a scheme and
   * authority is passed.
   */
  @Test
  public void uriCheckNoSchemeAuthority() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_RPC_PORT, "19998");

    AlluxioURI uri = new AlluxioURI("/root");
    mFileSystem.createDirectory(uri);

    assertFalse(mTestLogger.mAppender.wasLogged("The URI authority"));
    assertFalse(mTestLogger.mAppender.wasLogged("The URI scheme"));
  }

}

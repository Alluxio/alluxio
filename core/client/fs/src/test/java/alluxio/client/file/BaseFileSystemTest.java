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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.TestLoggerRule;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptions;
import alluxio.wire.FileInfo;

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

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Rule
  private TestLoggerRule mTestLogger = new TestLoggerRule();

  private FileSystem mFileSystem;
  private FileSystemContext mFileContext;
  private ClientContext mClientContext;
  private FileSystemMasterClient mFileSystemMasterClient;

  private class DummyAlluxioFileSystem extends BaseFileSystem {
    public DummyAlluxioFileSystem(FileSystemContext fsContext) {
      super(fsContext);
    }
  }

  /**
   * Sets up the file system and the context before a test runs.
   */
  @Before
  public void before() {
    mClientContext = ClientContext.create(mConf);
    mFileContext = PowerMockito.mock(FileSystemContext.class);
    mFileSystemMasterClient = PowerMockito.mock(FileSystemMasterClient.class);
    when(mFileContext.acquireMasterClientResource()).thenReturn(
        new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClient) {
          @Override
          public void close() {
            // Noop.
          }
        });
    when(mFileContext.getClientContext()).thenReturn(mClientContext);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mFileSystem = new DummyAlluxioFileSystem(mFileContext);
  }

  @After
  public void after() {
    mConf = ConfigurationTestUtils.defaults();
  }

  /**
   * Verifies and releases the master client after a test with a filesystem operation.
   */
  public void verifyFilesystemContextAcquiredAndReleased() {
    verify(mFileContext).acquireMasterClientResource();
  }

  /**
   * Tests the creation of a file via the
   * {@link BaseFileSystem#createFile(AlluxioURI, CreateFilePOptions)} method.
   */
  @Test
  public void createFile() throws Exception {
    URIStatus status = new URIStatus(new FileInfo());
    AlluxioURI file = new AlluxioURI("/file");
    when(mFileSystemMasterClient.createFile(any(AlluxioURI.class), any(CreateFilePOptions.class)))
        .thenReturn(status);
    mFileSystem.createFile(file, CreateFilePOptions.getDefaultInstance());
    verify(mFileSystemMasterClient).createFile(file, FileSystemOptions.createFileDefaults(mConf)
            .toBuilder().mergeFrom(CreateFilePOptions.getDefaultInstance()).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when creating a file system.
   */
  @Test
  public void createException() throws Exception {
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .createFile(any(AlluxioURI.class), any(CreateFilePOptions.class));
    try {
      mFileSystem.createFile(new AlluxioURI("/"), CreateFilePOptions.getDefaultInstance());
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#delete(AlluxioURI, DeletePOptions)} method.
   */
  @Test
  public void delete() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    DeletePOptions deleteOptions = DeletePOptions.newBuilder().setRecursive(true).build();
    mFileSystem.delete(file, deleteOptions);
    verify(mFileSystemMasterClient).delete(file,
        FileSystemOptions.deleteDefaults(mConf).toBuilder().mergeFrom(deleteOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when deleting a file.
   */
  @Test
  public void deleteException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    DeletePOptions deleteOptions = DeletePOptions.newBuilder().setRecursive(true).build();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).delete(file,
        FileSystemOptions.deleteDefaults(mConf)
            .toBuilder().mergeFrom(deleteOptions).build());
    try {
      mFileSystem.delete(file, deleteOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#free(AlluxioURI, FreePOptions)} method.
   */
  @Test
  public void free() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    FreePOptions freeOptions = FreePOptions.newBuilder().setRecursive(true).build();
    mFileSystem.free(file, freeOptions);
    verify(mFileSystemMasterClient).free(file, FileSystemOptions.freeDefaults(mConf)
        .toBuilder().mergeFrom(freeOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when freeing a file.
   */
  @Test
  public void freeException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    FreePOptions freeOptions = FreePOptions.newBuilder().setRecursive(true).build();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).free(file,
        FileSystemOptions.freeDefaults(mConf).toBuilder().mergeFrom(freeOptions).build());
    try {
      mFileSystem.free(file, freeOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#getStatus(AlluxioURI, GetStatusPOptions)} method.
   */
  @Test
  public void getStatus() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    URIStatus status = new URIStatus(new FileInfo());
    GetStatusPOptions getStatusOptions = GetStatusPOptions.getDefaultInstance();
    when(mFileSystemMasterClient.getStatus(file, FileSystemOptions.getStatusDefaults(mConf)
        .toBuilder().mergeFrom(getStatusOptions).build())).thenReturn(status);
    assertSame(status, mFileSystem.getStatus(file, getStatusOptions));
    verify(mFileSystemMasterClient).getStatus(file, FileSystemOptions.getStatusDefaults(mConf)
        .toBuilder().mergeFrom(getStatusOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when retrieving information.
   */
  @Test
  public void getStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusPOptions getStatusOptions = GetStatusPOptions.getDefaultInstance();
    when(mFileSystemMasterClient.getStatus(file, FileSystemOptions.getStatusDefaults(mConf)
        .toBuilder().mergeFrom(getStatusOptions).build())).thenThrow(EXCEPTION);
    try {
      mFileSystem.getStatus(file, getStatusOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#listStatus(AlluxioURI, ListStatusPOptions)} method.
   */
  @Test
  public void listStatus() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    List<URIStatus> infos = new ArrayList<>();
    infos.add(new URIStatus(new FileInfo()));
    ListStatusPOptions listStatusOptions = ListStatusPOptions.getDefaultInstance();
    when(mFileSystemMasterClient.listStatus(file, FileSystemOptions.listStatusDefaults(mConf)
        .toBuilder().mergeFrom(listStatusOptions).build())).thenReturn(infos);
    assertSame(infos, mFileSystem.listStatus(file, listStatusOptions));
    verify(mFileSystemMasterClient).listStatus(file, FileSystemOptions.listStatusDefaults(mConf)
        .toBuilder().mergeFrom(listStatusOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when listing the status.
   */
  @Test
  public void listStatusException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    when(mFileSystemMasterClient.listStatus(file, FileSystemOptions.listStatusDefaults(mConf)
        .toBuilder().mergeFrom(ListStatusPOptions.getDefaultInstance()).build()))
        .thenThrow(EXCEPTION);
    try {
      mFileSystem.listStatus(file, ListStatusPOptions.getDefaultInstance());
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

//  /**
//   * Tests for the {@link BaseFileSystem#loadMetadata(AlluxioURI, LoadMetadataOptions)}
//   * method.
//   */
//  @Test
//  public void loadMetadata() throws Exception {
//    AlluxioURI file = new AlluxioURI("/file");
//    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
//    doNothing().when(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
//    mFileSystem.loadMetadata(file, loadMetadataOptions);
//    verify(mFileSystemMasterClient).loadMetadata(file, loadMetadataOptions);
//
//    verifyFilesystemContextAcquiredAndReleased();
//  }
//
//  /**
//   * Ensures that an exception is propagated correctly when loading the metadata.
//   */
//  @Test
//  public void loadMetadataException() throws Exception {
//    AlluxioURI file = new AlluxioURI("/file");
//    LoadMetadataOptions loadMetadataOptions = LoadMetadataOptions.defaults().setRecursive(true);
//    doThrow(EXCEPTION).when(mFileSystemMasterClient)
//        .loadMetadata(file, loadMetadataOptions);
//    try {
//      mFileSystem.loadMetadata(file, loadMetadataOptions);
//      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
//    } catch (Exception e) {
//      assertSame(EXCEPTION, e);
//    }
//
//    verifyFilesystemContextAcquiredAndReleased();
//  }

  /**
   * Tests for the {@link BaseFileSystem#createDirectory(AlluxioURI, CreateDirectoryPOptions)}
   * method.
   */
  @Test
  public void createDirectory() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    CreateDirectoryPOptions createDirectoryOptions = CreateDirectoryPOptions.getDefaultInstance();
    doNothing().when(mFileSystemMasterClient).createDirectory(dir,
        FileSystemOptions.createDirectoryDefaults(mConf)
            .toBuilder().mergeFrom(createDirectoryOptions).build());
    mFileSystem.createDirectory(dir, createDirectoryOptions);
    verify(mFileSystemMasterClient).createDirectory(dir,
        FileSystemOptions.createDirectoryDefaults(mConf)
            .toBuilder().mergeFrom(createDirectoryOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when creating a directory.
   */
  @Test
  public void createDirectoryException() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    CreateDirectoryPOptions createDirectoryOptions = CreateDirectoryPOptions.getDefaultInstance();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).createDirectory(dir,
        FileSystemOptions.createDirectoryDefaults(mConf)
            .toBuilder().mergeFrom(createDirectoryOptions).build());
    try {
      mFileSystem.createDirectory(dir, createDirectoryOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#mount(AlluxioURI, AlluxioURI, MountPOptions)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountPOptions mountOptions = MountPOptions.getDefaultInstance();
    doNothing().when(mFileSystemMasterClient).mount(alluxioPath, ufsPath,
        FileSystemOptions.mountDefaults(mConf).toBuilder().mergeFrom(mountOptions).build());
    mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
    verify(mFileSystemMasterClient).mount(alluxioPath, ufsPath,
        FileSystemOptions.mountDefaults(mConf).toBuilder().mergeFrom(mountOptions).build());

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated correctly when mounting a path.
   */
  @Test
  public void mountException() throws Exception {
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountPOptions mountOptions = MountPOptions.getDefaultInstance();
    doThrow(EXCEPTION).when(mFileSystemMasterClient)
        .mount(alluxioPath, ufsPath,
            FileSystemOptions.mountDefaults(mConf).toBuilder().mergeFrom(mountOptions).build());
    try {
      mFileSystem.mount(alluxioPath, ufsPath, mountOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#openFile(AlluxioURI, OpenFilePOptions)} method to
   * complete successfully.
   */
  @Test
  public void openFile() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    URIStatus status = new URIStatus(new FileInfo().setCompleted(true));
    GetStatusPOptions getStatusOptions = getOpenOptions(GetStatusPOptions.getDefaultInstance());
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions))
        .thenReturn(status);
    mFileSystem.openFile(file, OpenFilePOptions.getDefaultInstance());
    verify(mFileSystemMasterClient).getStatus(file, getStatusOptions);

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Ensures that an exception is propagated successfully when opening a file.
   */
  @Test
  public void openException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    GetStatusPOptions getStatusOptions = getOpenOptions(GetStatusPOptions.getDefaultInstance());
    when(mFileSystemMasterClient.getStatus(file, getStatusOptions))
        .thenThrow(EXCEPTION);
    try {
      mFileSystem.openFile(file, OpenFilePOptions.getDefaultInstance());
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }

    verifyFilesystemContextAcquiredAndReleased();
  }

  /**
   * Tests for the {@link BaseFileSystem#rename(AlluxioURI, AlluxioURI, RenamePOptions)}
   * method.
   */
  @Test
  public void rename() throws Exception {
    AlluxioURI src = new AlluxioURI("/file");
    AlluxioURI dst = new AlluxioURI("/file2");
    RenamePOptions renameOptions = RenamePOptions.getDefaultInstance();
    doNothing().when(mFileSystemMasterClient).rename(src, dst,
        FileSystemOptions.renameDefaults(mConf).toBuilder().mergeFrom(renameOptions).build());
    mFileSystem.rename(src, dst, renameOptions);
    verify(mFileSystemMasterClient).rename(src, dst,
        FileSystemOptions.renameDefaults(mConf).toBuilder().mergeFrom(renameOptions).build());
  }

  /**
   * Ensures that an exception is propagated successfully when renaming a file.
   */
  @Test
  public void renameException() throws Exception {
    AlluxioURI src = new AlluxioURI("/file");
    AlluxioURI dst = new AlluxioURI("/file2");
    RenamePOptions renameOptions = RenamePOptions.getDefaultInstance();
    doThrow(EXCEPTION).when(mFileSystemMasterClient).rename(src, dst,
        FileSystemOptions.renameDefaults(mConf)
        .toBuilder().mergeFrom(renameOptions).build());
    try {
      mFileSystem.rename(src, dst, renameOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#setAttribute(AlluxioURI, SetAttributePOptions)} method.
   */
  @Test
  public void setAttribute() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    SetAttributePOptions setAttributeOptions =
        FileSystemOptions.setAttributeClientDefaults(mFileContext.getPathConf(file));
    mFileSystem.setAttribute(file, setAttributeOptions);
    verify(mFileSystemMasterClient).setAttribute(file, setAttributeOptions);
  }

  /**
   * Tests that the metadata sync interval is included on setAttributePOptions by default.
   */
  @Test
  public void setAttributeSyncMetadataInterval() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    SetAttributePOptions opt =
        FileSystemOptions.setAttributeClientDefaults(mFileContext.getPathConf(file));
    // Check that metadata sync interval from configuration is used when options are omitted
    mFileSystem.setAttribute(file);
    verify(mFileSystemMasterClient).setAttribute(file, opt);
  }

  /**
   * Ensures that an exception is propagated successfully when setting the state.
   */
  @Test
  public void setStateException() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    SetAttributePOptions setAttributeOptions =
        FileSystemOptions.setAttributeClientDefaults(mFileContext.getPathConf(file));
    doThrow(EXCEPTION).when(mFileSystemMasterClient).setAttribute(file, setAttributeOptions);
    try {
      mFileSystem.setAttribute(file, setAttributeOptions);
      fail(SHOULD_HAVE_PROPAGATED_MESSAGE);
    } catch (Exception e) {
      assertSame(EXCEPTION, e);
    }
  }

  /**
   * Tests for the {@link BaseFileSystem#unmount(AlluxioURI, UnmountPOptions)} method.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI path = new AlluxioURI("/");
    UnmountPOptions unmountOptions = UnmountPOptions.getDefaultInstance();
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
    UnmountPOptions unmountOptions = UnmountPOptions.getDefaultInstance();
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
    mConf.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    mConf.set(PropertyKey.MASTER_RPC_PORT, "19998");

    assertBadAuthority("localhost:1234", "Should fail on bad host and port");
    assertBadAuthority("zk@localhost:19998", "Should fail on zk authority");

    assertTrue(loggedAuthorityWarning());
    assertTrue(loggedSchemeWarning());
  }

  /**
   * Ensures an exception is thrown when an invalid scheme is passed.
   */
  @Test
  public void uriCheckBadScheme() throws Exception {
    mConf.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    mConf.set(PropertyKey.MASTER_RPC_PORT, "19998");

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
    mConf.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    mConf.set(PropertyKey.MASTER_RPC_PORT, "19998");
    before(); // Resets the filesystem and contexts to use proper configuration.

    useUriWithAuthority("localhost:19998");

    assertTrue(loggedAuthorityWarning());
    assertTrue(loggedSchemeWarning());
  }

  /**
   * Ensures there is no warnings or errors when an {@link AlluxioURI} without a scheme and
   * authority is passed.
   */
  @Test
  public void uriCheckNoSchemeAuthority() throws Exception {
    mConf.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    mConf.set(PropertyKey.MASTER_RPC_PORT, "19998");

    AlluxioURI uri = new AlluxioURI("/root");
    mFileSystem.createDirectory(uri);

    assertFalse(loggedAuthorityWarning());
    assertFalse(loggedSchemeWarning());
  }

  @Test
  public void uriCheckZkAuthorityMatch() throws Exception {
    configureZk("a:0,b:0,c:0");
    useUriWithAuthority("zk@a:0,b:0,c:0"); // Same authority
    useUriWithAuthority("zk@a:0;b:0+c:0"); // Same authority, but different delimiters
  }

  @Test
  public void uriCheckZkAuthorityMismatch() throws Exception {
    configureZk("a:0,b:0,c:0");

    assertBadAuthority("a:0,b:0,c:0", "Should fail on non-zk authority");
    assertBadAuthority("zk@a:0", "Should fail on zk authority with different addresses");
    assertBadAuthority("zk@a:0,b:0,c:1", "Should fail on zk authority with different addresses");
  }

  private GetStatusPOptions getOpenOptions(GetStatusPOptions getStatusOptions) {
    return FileSystemOptions.getStatusDefaults(mConf)
        .toBuilder().setAccessMode(Bits.READ).setUpdateTimestamps(true)
        .mergeFrom(getStatusOptions).build();
  }

  private void assertBadAuthority(String authority, String failureMessage) throws Exception {
    try {
      useUriWithAuthority(authority);
      fail(failureMessage);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("does not match"));
    }
  }

  private void useUriWithAuthority(String authority) throws Exception {
    mFileSystem.createDirectory(new AlluxioURI(String.format("alluxio://%s/dir", authority)));
  }

  private boolean loggedAuthorityWarning() {
    return mTestLogger.wasLogged("The URI authority .* is ignored");
  }

  private boolean loggedSchemeWarning() {
    return mTestLogger.wasLogged("The URI scheme .* is ignored");
  }

  private void configureZk(String addrs) {
    mConf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    mConf.set(PropertyKey.ZOOKEEPER_ADDRESS, addrs);
    before(); // Resets the filesystem and contexts to use proper configuration
  }
}

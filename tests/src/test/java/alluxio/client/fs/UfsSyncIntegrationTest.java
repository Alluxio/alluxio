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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.grpc.Context;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests the loading of metadata and the available options.
 */
public class UfsSyncIntegrationTest extends BaseIntegrationTest {
  private static final long INTERVAL_MS = 100;
  private static final long LARGE_INTERVAL_MS = 1000;
  private static final FileSystemMasterCommonPOptions PSYNC_NEVER =
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build();
  private static final FileSystemMasterCommonPOptions PSYNC_ALWAYS =
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build();
  private static final FileSystemMasterCommonPOptions PSYNC_INTERVAL =
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(INTERVAL_MS).build();
  private static final FileSystemMasterCommonPOptions PSYNC_LARGE_INTERVAL =
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(LARGE_INTERVAL_MS).build();

  private static final String ROOT_DIR = "/";
  private static final String EXISTING_DIR = "/dir_exist";
  private static final String NEW_FILE_UNDER_DIR = "/dir_exist/file_new";
  private static final String EXISTING_FILE = "/file_exist";
  private static final String NEW_DIR = "/dir_new";
  private static final String NEW_FILE = "/file_new";
  private static final String NEW_NESTED_FILE = "/a/b/file_new";

  private FileSystem mFileSystem;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test",
      ServerConfiguration.global());

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          // use a smaller block size so files can have multiple blocks
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, 5)
          .setProperty(PropertyKey.MASTER_METADATA_SYNC_CONCURRENCY_LEVEL, 10)
          .setProperty(PropertyKey.MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE, 10).build();

  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  @Before
  public void before() throws Exception {
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));

    new File(ufsPath(EXISTING_DIR)).mkdirs();
    writeUfsFile(ufsPath(EXISTING_FILE), 1);
  }

  @Test
  public void getStatusNoSync() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build();
    checkGetStatus(EXISTING_DIR, options, false);
    checkGetStatus(EXISTING_FILE, options, false);

    // Create new ufs paths.
    new File(ufsPath(NEW_DIR)).mkdirs();
    writeUfsFile(ufsPath(NEW_FILE), 2);

    checkGetStatus(NEW_DIR, options, false);
    checkGetStatus(NEW_FILE, options, false);
  }

  @Test
  public void listStatusNoSync() throws Exception {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build();
    checkListStatus(EXISTING_DIR, options, false);
    checkListStatus(EXISTING_FILE, options, false);

    // Create new ufs paths.
    new File(ufsPath(NEW_DIR)).mkdirs();
    writeUfsFile(ufsPath(NEW_FILE), 2);

    checkListStatus(NEW_DIR, options, false);
    checkListStatus(NEW_FILE, options, false);
  }

  @Test
  public void getStatusFileSync() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    checkGetStatus(EXISTING_FILE, options, true);
  }

  @Test
  public void getStatusDirSync() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    checkGetStatus(EXISTING_DIR, options, true);
  }

  // https://github.com/Alluxio/alluxio/issues/12372
  @Test
  public void getStatusDirSyncOnlyTouchingChildren() throws Exception {
    String dir1 = PathUtils.concatPath(EXISTING_DIR, "dir_should_sync");
    String dir2 = PathUtils.concatPath(dir1, "dir_should_not_sync");
    new File(ufsPath(dir1)).mkdirs();
    new File(ufsPath(dir2)).mkdirs();
    GetStatusPOptions optionsAlways = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(PSYNC_ALWAYS).build();
    checkGetStatus(EXISTING_DIR, optionsAlways, true);
    ListStatusPOptions optionsNever = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER)
        .setRecursive(false).build();
    checkListStatus(dir2, optionsNever, false);
  }

  @Test
  public void listDirSync() throws Exception {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    checkListStatus(ROOT_DIR, options, true);

    // Create new ufs paths.
    new File(ufsPath(NEW_DIR)).mkdirs();
    writeUfsFile(ufsPath(NEW_FILE), 2);

    checkListStatus(ROOT_DIR, options, true);
  }

  // https://github.com/Alluxio/alluxio/issues/12372
  @Test
  public void listDirSyncOnlyTouchingChildren() throws Exception {
    String dir1 = PathUtils.concatPath(EXISTING_DIR, "dir_should_sync");
    String dir2 = PathUtils.concatPath(dir1, "dir_should_not_sync");
    new File(ufsPath(dir1)).mkdirs();
    new File(ufsPath(dir2)).mkdirs();
    ListStatusPOptions optionsAlways = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setRecursive(false)
        .setCommonOptions(PSYNC_ALWAYS).build();
    checkListStatus(EXISTING_DIR, optionsAlways, true);
    ListStatusPOptions optionsNever = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER)
        .setRecursive(false).build();
    checkListStatus(dir2, optionsNever, false);
  }

  @Test
  public void getStatusFileSyncInterval() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_INTERVAL).build();
    long startMs = System.currentTimeMillis();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    long startLength = status.getLength();
    int sizeFactor = 10;
    while (true) {
      writeUfsFile(ufsPath(EXISTING_FILE), sizeFactor);
      status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
      if (status.getLength() != startLength) {
        break;
      }
      sizeFactor++;
    }
    long endMs = System.currentTimeMillis();

    assertTrue((endMs - startMs) >= INTERVAL_MS);
  }

  @Test(timeout = 10000)
  public void listDirSyncInterval() throws Exception {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_INTERVAL).build();
    long startMs = System.currentTimeMillis();
    List<URIStatus> statusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options);

    long startSize = statusList.size();
    int index = 10;
    while (true) {
      writeUfsFile(ufsPath(NEW_FILE + index), 1);
      statusList = mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options);
      if (statusList.size() != startSize) {
        break;
      }
      index++;
    }
    long endMs = System.currentTimeMillis();

    assertTrue((endMs - startMs) >= INTERVAL_MS);
  }

  @Test
  public void deleteFileNoSync() throws Exception {
    DeletePOptions options = DeletePOptions.newBuilder().setCommonOptions(PSYNC_NEVER).build();
    try {
      mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
      Assert.fail("Delete expected to fail: " + alluxioPath(EXISTING_FILE));
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  @Test
  public void deleteFileSync() throws Exception {
    DeletePOptions options = DeletePOptions.newBuilder().setCommonOptions(PSYNC_ALWAYS).build();
    mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
  }

  @Test
  public void renameFileNoSync() throws Exception {
    RenamePOptions options = RenamePOptions.newBuilder().setCommonOptions(PSYNC_NEVER).build();
    try {
      mFileSystem
          .rename(new AlluxioURI(alluxioPath(EXISTING_FILE)), new AlluxioURI(alluxioPath(NEW_FILE)),
              options);
      Assert.fail("Rename expected to fail.");
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  @Test
  public void renameFileSync() throws Exception {
    RenamePOptions options = RenamePOptions.newBuilder().setCommonOptions(PSYNC_ALWAYS).build();
    mFileSystem
        .rename(new AlluxioURI(alluxioPath(EXISTING_FILE)), new AlluxioURI(alluxioPath(NEW_FILE)),
            options);
  }

  @Test
  public void unpersistedFileSync() throws Exception {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    List<String> initialStatusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options).stream()
            .map(URIStatus::getName).collect(Collectors.toList());

    // write a MUST_CACHE file
    writeMustCacheFile(alluxioPath(NEW_FILE), 1);

    // List the status with force sync.
    List<String> syncStatusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options).stream()
            .map(URIStatus::getName).collect(Collectors.toList());

    Set<String> initialSet = Sets.newHashSet(initialStatusList);
    Set<String> syncSet = Sets.newHashSet(syncStatusList);
    assertTrue(syncSet.size() > initialSet.size());
    syncSet.removeAll(initialSet);

    // only the MUST_CACHE file should remain.
    assertTrue(syncSet.size() == 1);
    String file = syncSet.iterator().next();
    assertTrue(file.equals(new AlluxioURI(NEW_FILE).getName()));
  }

  @Test(timeout = 10000)
  public void lastModifiedLocalFileSync() throws Exception {
    int sizefactor = 10;
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    writeUfsFile(ufsPath(EXISTING_FILE), sizefactor);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String startFingerprint = status.getUfsFingerprint();
    long startLength = status.getLength();

    long modTime = new File(ufsPath(EXISTING_FILE)).lastModified();
    long newModTime = modTime;
    while (newModTime == modTime) {
      // Sleep for a bit for a different last mod time.
      // This could be flaky, so
      long delay = 100;
      CommonUtils.sleepMs(delay);

      // Write the same file, but with a different last mod time.
      writeUfsFile(ufsPath(EXISTING_FILE), sizefactor);
      newModTime = new File(ufsPath(EXISTING_FILE)).lastModified();
    }

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);

    // Verify the sizes are the same, but the fingerprints are different.
    // Will only work with local ufs, since local ufs uses the last mod time for the content hash.
    assertTrue(status.getLength() == startLength);
    Assert.assertNotEquals(startFingerprint, status.getUfsFingerprint());
  }

  @Test
  public void mountPoint() throws Exception {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build();
    List<String> rootListing =
        mFileSystem.listStatus(new AlluxioURI("/"), options).stream().map(URIStatus::getName)
            .collect(Collectors.toList());
    assertEquals(1, rootListing.size());
    assertEquals("mnt", rootListing.get(0));

    options = ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(PSYNC_ALWAYS).build();
    rootListing =
        mFileSystem.listStatus(new AlluxioURI("/"), options).stream().map(URIStatus::getName)
            .collect(Collectors.toList());
    assertEquals(1, rootListing.size());
    assertEquals("mnt", rootListing.get(0));
  }

  @Test
  public void mountPointConflict() throws Exception {
    GetStatusPOptions gsOptions = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/"), gsOptions);

    // add a UFS dir which conflicts with a mount point.
    String fromRootUfs = status.getUfsPath() + "/mnt";
    assertTrue(new File(fromRootUfs).mkdirs());

    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    List<URIStatus> rootListing = mFileSystem.listStatus(new AlluxioURI("/"), options);
    assertEquals(1, rootListing.size());
    assertEquals("mnt", rootListing.get(0).getName());
    Assert.assertNotEquals(fromRootUfs, rootListing.get(0).getUfsPath());
  }

  @Test
  public void mountPointNested() throws Exception {
    String ufsPath = Files.createTempDir().getAbsolutePath();
    mFileSystem.createDirectory(new AlluxioURI("/nested/mnt/"), CreateDirectoryPOptions.newBuilder()
        .setRecursive(true).setWriteType(WritePType.CACHE_THROUGH).build());
    mFileSystem.mount(new AlluxioURI("/nested/mnt/ufs"), new AlluxioURI(ufsPath));

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"), SetAttributePOptions.newBuilder()
        .setCommonOptions(PSYNC_ALWAYS.toBuilder().setTtl(55555)).setRecursive(true)
        .build());

    // Verify /nested/mnt/ dir has 1 mount point
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build();
    List<URIStatus> listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    assertEquals(1, listing.size());
    assertEquals("ufs", listing.get(0).getName());

    // Remove a directory in the parent UFS, which has a mount point descendant
    URIStatus status =
        mFileSystem.getStatus(new AlluxioURI("/nested/mnt/"), GetStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build());
    assertTrue(new File(status.getUfsPath()).delete());

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"), SetAttributePOptions.newBuilder()
        .setCommonOptions(PSYNC_ALWAYS.toBuilder().setTtl(44444)).setRecursive(true).build());

    // Verify /nested/mnt/ dir has 1 mount point
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    assertEquals(1, listing.size());
    assertEquals("ufs", listing.get(0).getName());

    // Remove a directory in the parent UFS, which has a mount point descendant
    status = mFileSystem.getStatus(new AlluxioURI("/nested/"), GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_NEVER).build());
    assertTrue(new File(status.getUfsPath()).delete());

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"), SetAttributePOptions.newBuilder()
        .setCommonOptions(PSYNC_ALWAYS.toBuilder().setTtl(44444)).setRecursive(true).build());

    // Verify /nested/mnt/ dir has 1 mount point
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    assertEquals(1, listing.size());
    assertEquals("ufs", listing.get(0).getName());

    // adding a file into the nested mount point
    writeUfsFile(ufsPath + "/nestedufs", 1);

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"), SetAttributePOptions.newBuilder()
        .setCommonOptions(PSYNC_ALWAYS.toBuilder().setTtl(44444)).setRecursive(true).build());
    // Verify /nested/mnt/ufs dir has 1 file
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/ufs"), options);
    assertEquals(1, listing.size());
    assertEquals("nestedufs", listing.get(0).getName());
  }

  @Test
  public void ufsModeSync() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    writeUfsFile(ufsPath(EXISTING_FILE), 10);
    // Set the ufs permissions
    File ufsFile = new File(ufsPath(EXISTING_FILE));
    assertTrue(ufsFile.setReadable(true, false));
    assertTrue(ufsFile.setWritable(true, false));
    assertTrue(ufsFile.setExecutable(true, false));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String startFingerprint = status.getUfsFingerprint();

    // Update ufs permissions
    assertTrue(ufsFile.setExecutable(false, false));

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);

    // Verify the fingerprints are different.
    Assert.assertNotEquals(startFingerprint, status.getUfsFingerprint());
  }

  @Test
  public void alluxioModeFingerprintUpdate() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.ONCE).setCommonOptions(PSYNC_NEVER).build();
    writeUfsFile(ufsPath(EXISTING_FILE), 10);
    Assert
        .assertNotNull(mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options));

    // Set initial alluxio permissions
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto()).build());

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String startFingerprint = status.getUfsFingerprint();

    // Change alluxio permissions
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        SetAttributePOptions.newBuilder().setMode(new Mode((short) 0655).toProto()).build());

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String endFingerprint = status.getUfsFingerprint();

    // Verify the fingerprints are different.
    Assert.assertNotEquals(startFingerprint, endFingerprint);
  }

  @Test
  public void ufsDirUpdatePermissions() throws Exception {
    new File(ufsPath("/dir1")).mkdirs();
    new File(ufsPath("/dir1/dir2")).mkdirs();
    String fileA = "/dir1/dir2/fileA";
    writeUfsFile(ufsPath(fileA), 1);

    // Set the mode for the directory
    FileUtils.changeLocalFilePermission(ufsPath("/dir1"), "rwxrwxrwx");

    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath("/dir1")), options);
    Assert.assertNotNull(status);

    assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxrwxrwx")),
        status.getMode());

    // Change the mode for the directory
    FileUtils.changeLocalFilePermission(ufsPath("/dir1"), "rwxr-xr-x");

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath("/dir1")), options);
    Assert.assertNotNull(status);

    assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxr-xr-x")),
        status.getMode());
  }

  @Test
  public void ufsMetadataContentChange() throws Exception {
    FileSystemTestUtils.loadFile(mFileSystem, alluxioPath(EXISTING_FILE));

    GetStatusPOptions options = GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(PSYNC_ALWAYS).build();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    Assert.assertNotNull(status);
    long prevFileid = status.getFileId();

    // Set the mode for the file
    FileUtils.changeLocalFilePermission(ufsPath(EXISTING_FILE), "rwxrwxrwx");

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    Assert.assertNotNull(status);

    // Make sure the mode is correctly updated with a metadata change only
    assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxrwxrwx")),
        status.getMode());

    // Change the permission of the file and the file id should not change
    assertEquals(prevFileid, status.getFileId());

    // Change the content of the file and the file id should change as a result because it is
    // deleted and reloaded.
    writeUfsFile(ufsPath(EXISTING_FILE), 2);

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    Assert.assertNotNull(status);
    Assert.assertNotEquals(prevFileid, status.getFileId());
  }

  @Test
  public void ufsDeleteSync() throws Exception {
    FileSystemTestUtils.loadFile(mFileSystem, alluxioPath(EXISTING_FILE));
    FileSystemUtils
        .waitForAlluxioPercentage(mFileSystem, new AlluxioURI(alluxioPath(EXISTING_FILE)), 100);
    new File(ufsPath(EXISTING_FILE)).delete();
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        ExistsPOptions.newBuilder().setCommonOptions(PSYNC_ALWAYS).build()));
    mFileSystem.free(new AlluxioURI("/"), FreePOptions.newBuilder().setRecursive(true).build());
    BlockMasterClient blockClient =
        BlockMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    CommonUtils.waitFor("data to be freed", () -> {
      try {
        return blockClient.getUsedBytes() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
  }

  @Test
  public void ufsDeleteChildrenSync() throws Exception {
    // Create a UFS directory with several files
    // Force each file to have many blocks
    String baseDir = "/base_dir";
    int numChildren = 20;
    new File(ufsPath(baseDir)).mkdirs();
    for (int i = 0; i < numChildren; i++) {
      writeUfsFile(ufsPath(baseDir + "/child" + i), 10000);
    }

    ListStatusPOptions lsOptions =
        ListStatusPOptions.newBuilder().setCommonOptions(PSYNC_ALWAYS).build();

    // initial sync
    List<URIStatus> statuses =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(baseDir)), lsOptions);
    assertEquals(numChildren, statuses.size());

    // Delete all the children from UFS only
    for (int i = 0; i < numChildren; i++) {
      new File(ufsPath(baseDir + "/child" + i)).delete();
    }

    // this is a parallel sync, check to see if BlockDeletionContext is thread-safe.
    statuses =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(baseDir)), lsOptions);
    assertTrue(statuses.isEmpty());
  }

  @Test
  public void createNestedFileSync() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");

    mFileSystem.createFile(new AlluxioURI(alluxioPath(NEW_NESTED_FILE)),
        CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
            .setCommonOptions(PSYNC_ALWAYS).setRecursive(true).build())
        .close();

    // Make sure we can create the nested file.
    Assert.assertNotNull(mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE))));
  }

  @Test
  public void clusterRestartSync() throws Exception {
    ListStatusPOptions listStatusPOptions = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(PSYNC_LARGE_INTERVAL).build();

    List<URIStatus> statusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(EXISTING_DIR)), listStatusPOptions);
    Assert.assertNotNull(statusList);
    assertEquals(0, statusList.size());
    mLocalAlluxioClusterResource.get().stopMasters();
    mLocalAlluxioClusterResource.get().startMasters();

    List<URIStatus> statusListAfterRestart =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(EXISTING_DIR)), listStatusPOptions);
    Assert.assertNotNull(statusListAfterRestart);
    assertEquals(0, statusListAfterRestart.size());

    writeUfsFile(ufsPath(NEW_FILE_UNDER_DIR), 1);

    List<URIStatus> statusListAgain =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(EXISTING_DIR)), listStatusPOptions);
    Assert.assertNotNull(statusListAgain);
    assertEquals(0, statusListAgain.size());

    Thread.sleep(LARGE_INTERVAL_MS);

    List<URIStatus> statusListAfterSleeping =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(EXISTING_DIR)), listStatusPOptions);
    Assert.assertNotNull(statusListAfterSleeping);
    assertEquals(1, statusListAfterSleeping.size());
  }

  /** This is a timing based test and may become flaky.
   *  The goal is to simulate a user interrupted listStatus call.
   *
   *  In this case, the user's listStatus should have synced the first level directory but have
   *  not completed the second level directory sync. Thus resulting in a partial sync.
   */
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "NEVER"
      })
  @Test
  public void interruptSync() throws Exception {
    // make large nested directories/files in UFS
    for (int i = 0; i < 100; i++) {
      new File(ufsPath("/dir" + i)).mkdirs();
      for (int j = 0; j < 100; j++) {
        new File(ufsPath("/dir" + i + "/dir" + j)).mkdirs();
        writeUfsFile(ufsPath("/dir" + i + "/dir" + j + "/file"), 1);
      }
    }
    List<URIStatus> status;
    try (Context.CancellableContext c = Context.current()
        .withDeadlineAfter(1, TimeUnit.MILLISECONDS,
            Executors.newScheduledThreadPool(1))) {
      Context toRestore = c.attach();
      try {
        status = Context.current().withCancellation().call(() -> {
          try {
            return mFileSystem.listStatus(new AlluxioURI("/"), ListStatusPOptions.newBuilder()
                .setRecursive(true)
                .setCommonOptions(FileSystemOptions.commonDefaults(
                    mFileSystem.getConf()).toBuilder().setSyncIntervalMs(0).build()).build());
          } catch (Exception e) {
            return Collections.<URIStatus>emptyList();
          }
        });
        Thread.sleep(5);
        c.cancel(new AlluxioException("test exception"));
      } finally {
        c.detach(toRestore);
      }
    }
    // cancelled call should not return any results
    assertEquals(0, status.size());
    status = mFileSystem.listStatus(new AlluxioURI("/"), ListStatusPOptions.newBuilder()
        .setRecursive(true)
        .setCommonOptions(FileSystemOptions.commonDefaults(
            mFileSystem.getConf()).toBuilder().setSyncIntervalMs(-1).build()).build());
    final int TOTAL_FILE_COUNT = 20103;
    // verify that the previous sync did not complete
    assertTrue(status.size() < TOTAL_FILE_COUNT);
    for (URIStatus stat : status) {
      assertTrue(stat.isCompleted());
    }
    status = mFileSystem.listStatus(new AlluxioURI("/"), ListStatusPOptions.newBuilder()
        .setRecursive(true).setCommonOptions(PSYNC_ALWAYS).build());
    assertEquals(TOTAL_FILE_COUNT, status.size());
  }

  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "NEVER"
      })
  @Test
  public void recursiveSync() throws Exception {

    // make nested directories/files in UFS
    new File(ufsPath("/dir1")).mkdirs();
    new File(ufsPath("/dir1/dir2")).mkdirs();
    new File(ufsPath("/dir1/dir2/dir3")).mkdirs();
    String fileA = "/dir1/dir2/fileA";
    String fileB = "/dir1/dir2/fileB";
    String fileC = "/dir1/dir2/fileC";
    writeUfsFile(ufsPath(fileA), 1);
    writeUfsFile(ufsPath(fileB), 1);

    // Should not exist, since no loading or syncing
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileA)), ExistsPOptions.newBuilder()
        .setCommonOptions(FileSystemOptions.commonDefaults(mFileSystem.getConf()).toBuilder()
            .setSyncIntervalMs(-1).build()).build()));

    try {
      mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
          SetAttributePOptions.newBuilder().setRecursive(true).setCommonOptions(
              FileSystemMasterCommonPOptions.newBuilder().setTtl(55555).setSyncIntervalMs(-1))
              .build());
    } catch (FileDoesNotExistException e) {
      // expected, continue
    }

    // Enable UFS sync, before next recursive setAttribute.
    FileSystemMasterCommonPOptions ttlOption = FileSystemMasterCommonPOptions.newBuilder()
        .setTtl(123456789).setSyncIntervalMs(0).build();
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
        SetAttributePOptions.newBuilder().setRecursive(true).setCommonOptions(ttlOption).build());

    // Verify recursive set TTL by getting info, without sync.
    ttlOption = ttlOption.toBuilder().setSyncIntervalMs(-1).build();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(fileA)));
    assertEquals(ttlOption.getTtl(), status.getTtl());

    // Add UFS fileC and remove existing UFS fileA.
    writeUfsFile(ufsPath(fileC), 1);
    assertTrue(new File(ufsPath(fileA)).delete());

    // Enable UFS sync, before next recursive setAttribute.
    ttlOption = FileSystemMasterCommonPOptions.newBuilder().setTtl(987654321)
        .setSyncIntervalMs(0).build();
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
        SetAttributePOptions.newBuilder().setRecursive(true).setCommonOptions(ttlOption).build());

    // Verify recursive set TTL by getting info, without sync.
    ttlOption =
        FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).setTtl(987654321).build();
    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(fileB)));
    assertEquals(ttlOption.getTtl(), status.getTtl());

    // deleted UFS file should not exist.
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileA))));
  }

  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "NEVER"
      })
  @Test
  public void recursiveSyncCacheDescendants() throws Exception {
    // make nested directories/files in UFS
    new File(ufsPath("/dir1")).mkdirs();
    new File(ufsPath("/dir1/dir2")).mkdirs();
    new File(ufsPath("/dir1/dir2/dir3")).mkdirs();
    String fileA = "/dir1/dir2/dir3/fileA";
    String fileB = "/dir1/dir2/dir3/fileB";
    String fileNew = "/dir1/dir2/dir3/fileNew";
    writeUfsFile(ufsPath(fileA), 1);
    writeUfsFile(ufsPath(fileB), 1);

    FileSystemMasterCommonPOptions longinterval =
        FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(Constants.HOUR_MS).build();

    // Should not exist, since no loading or syncing
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileA)), ExistsPOptions.newBuilder()
        .setCommonOptions(PSYNC_NEVER).build()));

    try {
      mFileSystem.listStatus(new AlluxioURI(alluxioPath("/dir1")),
          ListStatusPOptions.newBuilder().setCommonOptions(PSYNC_NEVER).build());
      Assert.fail("paths are not expected to exist without sync");
    } catch (FileDoesNotExistException e) {
      // expected, continue
    }

    // recursively sync the top dir
    List<URIStatus> paths = mFileSystem.listStatus(new AlluxioURI(alluxioPath("/dir1")),
        ListStatusPOptions.newBuilder().setCommonOptions(PSYNC_ALWAYS).setRecursive(true)
            .build());
    assertEquals(4, paths.size());

    // write a new UFS file
    writeUfsFile(ufsPath(fileNew), 1);
    // the new UFS file should not exist, since the sync interval is 1 hour, and an ancestor
    // already synced recently.
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileNew)), ExistsPOptions.newBuilder()
        .setCommonOptions(longinterval).build()));

    // newly created file should not exist
    paths = mFileSystem.listStatus(new AlluxioURI(alluxioPath("/dir1/dir2/dir3")),
        ListStatusPOptions.newBuilder().setCommonOptions(longinterval).build());
    assertEquals(2, paths.size());

    // create a new UFS dir
    new File(ufsPath("/dir1/dir2/dirNew")).mkdirs();
    // newly created dir should not exist, since sync interval is long, and an ancestor is
    // already synced
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath("/dir1/dir2/dirNew")),
        ExistsPOptions.newBuilder().setCommonOptions(longinterval).build()));
    // newly created dir should not exist
    paths = mFileSystem.listStatus(new AlluxioURI(alluxioPath("/dir1/dir2")),
        ListStatusPOptions.newBuilder().setCommonOptions(longinterval).build());
    assertEquals(1, paths.size());

    // check the original path, and verify no new files/dirs are picked up from UFS
    paths = mFileSystem.listStatus(new AlluxioURI(alluxioPath("/dir1")),
        ListStatusPOptions.newBuilder().setCommonOptions(longinterval).setRecursive(true)
            .build());
    assertEquals(4, paths.size());
  }

  @Test
  public void deleteUfsFileGetStatus() throws Exception {
    new File(ufsPath("/delete")).mkdirs();
    writeUfsFile(ufsPath("/delete/file"), 10);

    List<URIStatus> paths = mFileSystem.listStatus(new AlluxioURI(alluxioPath("/delete")),
        ListStatusPOptions.newBuilder().setRecursive(false).setCommonOptions(PSYNC_ALWAYS).build());

    assertEquals(1, paths.size());
    assertEquals("file", paths.get(0).getName());

    // delete the file and wait a bit
    new File(ufsPath("/delete/file")).delete();
    CommonUtils.sleepMs(2000);

    // getStatus (not listStatus) on the root, with a shorter interval than the sleep.
    // This will sync that directory. The sync interval has to be long enough for the internal
    // syncing process to finish within that time.
    mFileSystem.getStatus(new AlluxioURI(alluxioPath("/delete")), GetStatusPOptions.newBuilder()
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(1000).build()).build());

    // verify that the file is deleted, without syncing
    try {
      mFileSystem.getStatus(new AlluxioURI(alluxioPath("/delete/file")),
          GetStatusPOptions.newBuilder().setCommonOptions(PSYNC_NEVER).build());
      Assert.fail("the ufs deleted file is not expected to exist after sync via getStatus");
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  private String ufsPath(String path) {
    return mLocalUfsPath + path;
  }

  private String alluxioPath(String path) {
    return "/mnt/" + path;
  }

  private void writeUfsFile(String path, int sizeFactor) throws IOException {
    FileWriter fileWriter = new FileWriter(path);
    for (int i = 0; i < sizeFactor; i++) {
      fileWriter.write("test");
    }
    fileWriter.close();
  }

  private void writeMustCacheFile(String path, int sizeFactor) throws Exception {
    CreateFilePOptions options = CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.MUST_CACHE).setRecursive(true).build();
    FileOutStream os = mFileSystem.createFile(new AlluxioURI(path), options);
    for (int i = 0; i < sizeFactor; i++) {
      os.write("test".getBytes());
    }
    os.close();
  }

  private void checkGetStatus(String path, GetStatusPOptions options, boolean expectExists)
      throws Exception {
    try {
      URIStatus uriStatus = mFileSystem.getStatus(new AlluxioURI(alluxioPath(path)), options);
      if (!expectExists) {
        Assert.fail("Path is not expected to exist: " + alluxioPath(path));
      }
      checkUriStatus(uriStatus);
    } catch (FileDoesNotExistException e) {
      if (expectExists) {
        throw e;
      }
    }
  }

  private void checkListStatus(String path, ListStatusPOptions options, boolean expectExists)
      throws Exception {
    try {
      List<URIStatus> statusList =
          mFileSystem.listStatus(new AlluxioURI(alluxioPath(path)), options);
      if (!expectExists) {
        Assert.fail("Path is not expected to exist: " + alluxioPath(path));
      }
      Assert.assertNotNull(statusList);
      for (URIStatus uriStatus : statusList) {
        checkUriStatus(uriStatus);
      }
      checkUfsListing(ufsPath(path), statusList);
    } catch (FileDoesNotExistException e) {
      if (expectExists) {
        throw e;
      }
    }
  }

  private void checkUriStatus(URIStatus uriStatus) {
    File ufsFile = new File(uriStatus.getUfsPath());
    if (uriStatus.isFolder() != ufsFile.isDirectory()) {
      Assert.fail("Directory mismatch (Alluxio isDir: " + uriStatus.isFolder() + ") (ufs isDir: "
          + ufsFile.isDirectory() + ") path: " + uriStatus.getPath());
    }
    if (!uriStatus.isFolder()) {
      long ufsLength = ufsFile.length();
      long alluxioLength = uriStatus.getLength();
      if (ufsLength != alluxioLength) {
        Assert.fail("Alluxio length (" + alluxioLength + ") and ufs length (" + ufsLength
            + ") are inconsistent. path: " + uriStatus.getPath());
      }
      // Check fingerprint.
      UnderFileSystem ufs = UnderFileSystem.Factory.create(uriStatus.getUfsPath(),
          ServerConfiguration.global());
      String ufsFingerprint = ufs.getFingerprint(uriStatus.getUfsPath());
      String alluxioFingerprint = uriStatus.getUfsFingerprint();
      if (!ufsFingerprint.equals(alluxioFingerprint)) {
        Assert.fail(
            "Alluxio fingerprint (" + alluxioFingerprint + ") and ufs fingerprint ("
                + ufsFingerprint + ") are inconsistent. path: " + uriStatus.getPath());
      }
    }
  }

  private void checkUfsListing(String ufsPath, List<URIStatus> statusList) {
    File ufsFile = new File(ufsPath);

    Set<String> ufsFiles = Sets.newHashSet(ufsFile.list());

    Set<String> alluxioFiles = new HashSet<>();
    for (URIStatus uriStatus : statusList) {
      alluxioFiles.add(uriStatus.getName());
    }

    ufsFiles.removeAll(alluxioFiles);
    if (!ufsFiles.isEmpty()) {
      Assert.fail("UFS files are missing from Alluxio: " + ufsFiles);
    }
  }
}

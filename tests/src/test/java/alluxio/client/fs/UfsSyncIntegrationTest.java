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

import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.MasterClientConfig;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests the loading of metadata and the available options.
 */
public class UfsSyncIntegrationTest extends BaseIntegrationTest {
  private static final long INTERVAL_MS = 100;
  private static final CommonOptions SYNC_NEVER = CommonOptions.defaults().setSyncIntervalMs(-1);
  private static final CommonOptions SYNC_ALWAYS = CommonOptions.defaults().setSyncIntervalMs(0);
  private static final CommonOptions SYNC_INTERVAL =
      CommonOptions.defaults().setSyncIntervalMs(INTERVAL_MS);
  private static final String ROOT_DIR = "/";
  private static final String EXISTING_DIR = "/dir_exist";
  private static final String EXISTING_FILE = "/file_exist";
  private static final String NEW_DIR = "/dir_new";
  private static final String NEW_FILE = "/file_new";
  private static final String NEW_NESTED_FILE = "/a/b/file_new";

  private FileSystem mFileSystem;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Before
  public void before() throws Exception {
    mFileSystem = FileSystem.Factory.get();
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));

    new File(ufsPath(EXISTING_DIR)).mkdirs();
    writeUfsFile(ufsPath(EXISTING_FILE), 1);
  }

  @Test
  public void getStatusNoSync() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER);
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
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER);
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
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    checkGetStatus(EXISTING_FILE, options, true);
  }

  @Test
  public void getStatusDirSync() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    checkGetStatus(EXISTING_DIR, options, true);
  }

  @Test
  public void listDirSync() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    checkListStatus(ROOT_DIR, options, true);

    // Create new ufs paths.
    new File(ufsPath(NEW_DIR)).mkdirs();
    writeUfsFile(ufsPath(NEW_FILE), 2);

    checkListStatus(ROOT_DIR, options, true);
  }

  @Test
  public void getStatusFileSyncInterval() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_INTERVAL);
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

    Assert.assertTrue((endMs - startMs) >= INTERVAL_MS);
  }

  @Test(timeout = 10000)
  public void listDirSyncInterval() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_INTERVAL);
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

    Assert.assertTrue((endMs - startMs) >= INTERVAL_MS);
  }

  @Test
  public void deleteFileNoSync() throws Exception {
    DeleteOptions options = DeleteOptions.defaults().setCommonOptions(SYNC_NEVER);
    try {
      mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
      Assert.fail("Delete expected to fail: " + alluxioPath(EXISTING_FILE));
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  @Test
  public void deleteFileSync() throws Exception {
    DeleteOptions options = DeleteOptions.defaults().setCommonOptions(SYNC_ALWAYS);
    mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
  }

  @Test
  public void renameFileNoSync() throws Exception {
    RenameOptions options = RenameOptions.defaults().setCommonOptions(SYNC_NEVER);
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
    RenameOptions options = RenameOptions.defaults().setCommonOptions(SYNC_ALWAYS);
    mFileSystem
        .rename(new AlluxioURI(alluxioPath(EXISTING_FILE)), new AlluxioURI(alluxioPath(NEW_FILE)),
            options);
  }

  @Test
  public void unpersistedFileSync() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
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
    Assert.assertTrue(syncSet.size() > initialSet.size());
    syncSet.removeAll(initialSet);

    // only the MUST_CACHE file should remain.
    Assert.assertTrue(syncSet.size() == 1);
    String file = syncSet.iterator().next();
    Assert.assertTrue(file.equals(new AlluxioURI(NEW_FILE).getName()));
  }

  @Test(timeout = 10000)
  public void lastModifiedLocalFileSync() throws Exception {
    int sizefactor = 10;
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
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
    Assert.assertTrue(status.getLength() == startLength);
    Assert.assertNotEquals(startFingerprint, status.getUfsFingerprint());
  }

  @Test
  public void mountPoint() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER);
    List<String> rootListing =
        mFileSystem.listStatus(new AlluxioURI("/"), options).stream().map(URIStatus::getName)
            .collect(Collectors.toList());
    Assert.assertEquals(1, rootListing.size());
    Assert.assertEquals("mnt", rootListing.get(0));

    options = ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
        .setCommonOptions(SYNC_ALWAYS);
    rootListing =
        mFileSystem.listStatus(new AlluxioURI("/"), options).stream().map(URIStatus::getName)
            .collect(Collectors.toList());
    Assert.assertEquals(1, rootListing.size());
    Assert.assertEquals("mnt", rootListing.get(0));
  }

  @Test
  public void mountPointConflict() throws Exception {
    GetStatusOptions getStatusOptions =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/"), getStatusOptions);

    // add a UFS dir which conflicts with a mount point.
    String fromRootUfs = status.getUfsPath() + "/mnt";
    Assert.assertTrue(new File(fromRootUfs).mkdirs());

    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    List<URIStatus> rootListing = mFileSystem.listStatus(new AlluxioURI("/"), options);
    Assert.assertEquals(1, rootListing.size());
    Assert.assertEquals("mnt", rootListing.get(0).getName());
    Assert.assertNotEquals(fromRootUfs, rootListing.get(0).getUfsPath());
  }

  @Test
  public void mountPointNested() throws Exception {
    String ufsPath = Files.createTempDir().getAbsolutePath();
    mFileSystem.createDirectory(new AlluxioURI("/nested/mnt/"),
        CreateDirectoryOptions.defaults().setRecursive(true).setWriteType(WriteType.CACHE_THROUGH));
    mFileSystem.mount(new AlluxioURI("/nested/mnt/ufs"), new AlluxioURI(ufsPath));

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"),
        SetAttributeOptions.defaults().setCommonOptions(SYNC_ALWAYS).setRecursive(true)
            .setTtl(55555));

    // Verify /nested/mnt/ dir has 1 mount point
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER);
    List<URIStatus> listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals("ufs", listing.get(0).getName());

    // Remove a directory in the parent UFS, which has a mount point descendant
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/nested/mnt/"),
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER));
    Assert.assertTrue(new File(status.getUfsPath()).delete());

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"),
        SetAttributeOptions.defaults().setCommonOptions(SYNC_ALWAYS).setRecursive(true)
            .setTtl(44444));

    // Verify /nested/mnt/ dir has 1 mount point
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals("ufs", listing.get(0).getName());

    // Remove a directory in the parent UFS, which has a mount point descendant
    status = mFileSystem.getStatus(new AlluxioURI("/nested/"),
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_NEVER));
    Assert.assertTrue(new File(status.getUfsPath()).delete());

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"),
        SetAttributeOptions.defaults().setCommonOptions(SYNC_ALWAYS).setRecursive(true)
            .setTtl(44444));

    // Verify /nested/mnt/ dir has 1 mount point
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/"), options);
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals("ufs", listing.get(0).getName());

    // adding a file into the nested mount point
    writeUfsFile(ufsPath + "/nestedufs", 1);

    // recursively sync (setAttribute enables recursive sync)
    mFileSystem.setAttribute(new AlluxioURI("/"),
        SetAttributeOptions.defaults().setCommonOptions(SYNC_ALWAYS).setRecursive(true)
            .setTtl(44444));
    // Verify /nested/mnt/ufs dir has 1 file
    listing = mFileSystem.listStatus(new AlluxioURI("/nested/mnt/ufs"), options);
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals("nestedufs", listing.get(0).getName());
  }

  @Test
  public void ufsModeSync() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    writeUfsFile(ufsPath(EXISTING_FILE), 10);
    // Set the ufs permissions
    File ufsFile = new File(ufsPath(EXISTING_FILE));
    Assert.assertTrue(ufsFile.setReadable(true, false));
    Assert.assertTrue(ufsFile.setWritable(true, false));
    Assert.assertTrue(ufsFile.setExecutable(true, false));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String startFingerprint = status.getUfsFingerprint();

    // Update ufs permissions
    Assert.assertTrue(ufsFile.setExecutable(false, false));

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);

    // Verify the fingerprints are different.
    Assert.assertNotEquals(startFingerprint, status.getUfsFingerprint());
  }

  @Test
  public void alluxioModeFingerprintUpdate() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Once)
            .setCommonOptions(SYNC_NEVER);
    writeUfsFile(ufsPath(EXISTING_FILE), 10);
    Assert
        .assertNotNull(mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options));

    // Set initial alluxio permissions
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        SetAttributeOptions.defaults().setMode(new Mode((short) 0777)));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    String startFingerprint = status.getUfsFingerprint();

    // Change alluxio permissions
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        SetAttributeOptions.defaults().setMode(new Mode((short) 0655)));

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

    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath("/dir1")), options);
    Assert.assertNotNull(status);

    Assert.assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxrwxrwx")),
        status.getMode());

    // Change the mode for the directory
    FileUtils.changeLocalFilePermission(ufsPath("/dir1"), "rwxr-xr-x");

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath("/dir1")), options);
    Assert.assertNotNull(status);

    Assert.assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxr-xr-x")),
        status.getMode());
  }

  @Test
  public void ufsMetadataContentChange() throws Exception {
    FileSystemTestUtils.loadFile(mFileSystem, alluxioPath(EXISTING_FILE));

    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(SYNC_ALWAYS);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    Assert.assertNotNull(status);
    long prevFileid = status.getFileId();

    // Set the mode for the file
    FileUtils.changeLocalFilePermission(ufsPath(EXISTING_FILE), "rwxrwxrwx");

    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    Assert.assertNotNull(status);

    // Make sure the mode is correctly updated with a metadata change only
    Assert.assertEquals(
        FileUtils.translatePosixPermissionToMode(PosixFilePermissions.fromString("rwxrwxrwx")),
        status.getMode());

    // Change the permission of the file and the file id should not change
    Assert.assertEquals(prevFileid, status.getFileId());

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
    new File(ufsPath(EXISTING_FILE)).delete();
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(EXISTING_FILE)),
        ExistsOptions.defaults().setCommonOptions(SYNC_ALWAYS)));
    mFileSystem.free(new AlluxioURI("/"), FreeOptions.defaults().setRecursive(true));
    BlockMasterClient blockClient = BlockMasterClient.Factory.create(MasterClientConfig.defaults());
    CommonUtils.waitFor("data to be freed", (x) -> {
      try {
        return blockClient.getUsedBytes() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void createNestedFileSync() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");

    mFileSystem.createFile(new AlluxioURI(alluxioPath(NEW_NESTED_FILE)),
        CreateFileOptions.defaults().setRecursive(true).setWriteType(WriteType.CACHE_THROUGH)
            .setCommonOptions(SYNC_ALWAYS)).close();

    // Make sure we can create the nested file.
    Assert.assertNotNull(mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE))));
  }

  @Test
  public void recursiveSync() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1");
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, "Never");

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
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileA))));

    try {
      mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
          SetAttributeOptions.defaults().setRecursive(true).setTtl(55555));
    } catch (FileDoesNotExistException e) {
      // expected, continue
    }

    // Enable UFS sync, before next recursive setAttribute.
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");
    long ttl = 123456789;
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
        SetAttributeOptions.defaults().setRecursive(true).setTtl(ttl));

    // Verify recursive set TTL by getting info, without sync.
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1");
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(fileA)));
    Assert.assertEquals(ttl, status.getTtl());

    // Add UFS fileC and remove existing UFS fileA.
    writeUfsFile(ufsPath(fileC), 1);
    Assert.assertTrue(new File(ufsPath(fileA)).delete());

    // Enable UFS sync, before next recursive setAttribute.
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "0");
    ttl = 987654321;
    mFileSystem.setAttribute(new AlluxioURI(alluxioPath("/dir1")),
        SetAttributeOptions.defaults().setRecursive(true).setTtl(ttl));

    // Verify recursive set TTL by getting info, without sync.
    Configuration.set(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL, "-1");
    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(fileB)));
    Assert.assertEquals(ttl, status.getTtl());

    // deleted UFS file should not exist.
    assertFalse(mFileSystem.exists(new AlluxioURI(alluxioPath(fileA))));
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
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE);
    FileOutStream os = mFileSystem.createFile(new AlluxioURI(path), options);
    for (int i = 0; i < sizeFactor; i++) {
      os.write("test".getBytes());
    }
    os.close();
  }

  private void checkGetStatus(String path, GetStatusOptions options, boolean expectExists)
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

  private void checkListStatus(String path, ListStatusOptions options, boolean expectExists)
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
      UnderFileSystem ufs = UnderFileSystem.Factory.create(uriStatus.getUfsPath());
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

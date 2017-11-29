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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.BaseIntegrationTest;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.util.CommonUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests the loading of metadata and the available options.
 */
public class UfsSyncIntegrationTest extends BaseIntegrationTest {
  private static final long INTERVAL_MS = Constants.SECOND_MS;
  private static final String ROOT_DIR = "/";
  private static final String EXISTING_DIR = "/dir_exist";
  private static final String EXISTING_FILE = "/file_exist";
  private static final String NEW_DIR = "/dir_new";
  private static final String NEW_FILE = "/file_new";

  private FileSystem mFileSystem;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws Exception {
    mFileSystem = FileSystem.Factory.get();
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI(mLocalUfsPath));

    new File(ufsPath(EXISTING_DIR)).mkdirs();
    writeUfsFile(ufsPath(EXISTING_FILE), 1);
  }

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void getStatusNoSync() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never).setSyncInterval(-1);
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
            .setSyncInterval(-1);
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
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never).setSyncInterval(0);
    checkGetStatus(EXISTING_FILE, options, true);
  }

  @Test
  public void getStatusDirSync() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never).setSyncInterval(0);
    checkGetStatus(EXISTING_DIR, options, true);
  }

  @Test
  public void listDirSync() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never).setSyncInterval(0);
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
            .setSyncInterval(INTERVAL_MS);
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

  @Test
  public void listDirSyncInterval() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setSyncInterval(INTERVAL_MS);
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
    DeleteOptions options = DeleteOptions.defaults().setSyncInterval(-1);
    try {
      mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
      Assert.fail("Delete expected to fail: " + alluxioPath(EXISTING_FILE));
    } catch (FileDoesNotExistException e) {
      // expected
    }
  }

  @Test
  public void deleteFileSync() throws Exception {
    DeleteOptions options = DeleteOptions.defaults().setSyncInterval(0);
    mFileSystem.delete(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
  }

  @Test
  public void renameFileNoSync() throws Exception {
    RenameOptions options = RenameOptions.defaults().setSyncInterval(-1);
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
    RenameOptions options = RenameOptions.defaults().setSyncInterval(0);
    mFileSystem
        .rename(new AlluxioURI(alluxioPath(EXISTING_FILE)), new AlluxioURI(alluxioPath(NEW_FILE)),
            options);
  }

  @Test
  public void unpersistedFileSync() throws Exception {
    ListStatusOptions options =
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never).setSyncInterval(0);
    List<URIStatus> initialStatusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options);

    // write a MUST_CACHE file
    writeMustCacheFile(alluxioPath(NEW_FILE), 1);

    // List the status with force sync.
    List<URIStatus> syncStatusList =
        mFileSystem.listStatus(new AlluxioURI(alluxioPath(ROOT_DIR)), options);

    Set<URIStatus> initialSet = Sets.newHashSet(initialStatusList);
    Set<URIStatus> syncSet = Sets.newHashSet(syncStatusList);
    Assert.assertTrue(syncSet.size() > initialSet.size());
    syncSet.removeAll(initialSet);

    // only the MUST_CACHE file should remain.
    Assert.assertTrue(syncSet.size() == 1);
    URIStatus file = syncSet.iterator().next();
    Assert.assertTrue(file.getName().equals(new AlluxioURI(NEW_FILE).getName()));
  }

  @Test
  public void lastModifiedFileSync() throws Exception {
    int sizefactor = 10;
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setSyncInterval(0);
    writeUfsFile(ufsPath(EXISTING_FILE), sizefactor);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);
    long startLength = status.getLength();
    long startLastMod = status.getUfsLastModificationTimeMs();

    // Sleep for a bit for a different last mod time.
    long delay = 1000;
    CommonUtils.sleepMs(delay);

    // Write the same file, but with a different last mod time.
    writeUfsFile(ufsPath(EXISTING_FILE), sizefactor);
    status = mFileSystem.getStatus(new AlluxioURI(alluxioPath(EXISTING_FILE)), options);

    // Verify the sizes are the same, but the last mod times are not.
    Assert.assertTrue(status.getLength() == startLength);
    Assert.assertTrue(status.getUfsLastModificationTimeMs() - startLastMod >= delay);
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
      long ufsLastMod = ufsFile.lastModified();
      long alluxioLastMod = uriStatus.getUfsLastModificationTimeMs();
      if (ufsLastMod != alluxioLastMod) {
        Assert.fail(
            "Alluxio last modified time (" + alluxioLastMod + ") and ufs last modified time ("
                + ufsLastMod + ") are inconsistent. path: " + uriStatus.getPath());
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

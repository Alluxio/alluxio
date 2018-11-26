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

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.LoadMetadataType;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

/**
 * Tests the loading of metadata and the available options.
 */
public class LoadMetadataIntegrationTest extends BaseIntegrationTest {
  private static final long SLEEP_MS = Constants.SECOND_MS / 2;

  private static final long LONG_SLEEP_MS = Constants.SECOND_MS * 2;

  private FileSystem mFileSystem;
  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions()
              .setExistsMs(SLEEP_MS)
              .setListStatusWithOptionsMs(LONG_SLEEP_MS)));

  @Before
  public void before() throws Exception {
    mFileSystem = FileSystem.Factory.get();
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI("sleep://" + mLocalUfsPath));

    new File(mLocalUfsPath + "/dir1/dirA/").mkdirs();
    FileWriter fileWriter = new FileWriter(mLocalUfsPath + "/dir1/dirA/file");
    fileWriter.write("test");
    fileWriter.close();
  }

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void loadMetadataAlways() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, true);
  }

  @Test
  public void loadMetadataNever() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/file", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/fileDNE3", options, false, false);
  }

  @Test
  public void loadMetadataOnce() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Once);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir1/file1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir2", options, false, false);
  }

  @Test
  public void loadMetadataOnceAfterUfsCreate() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Once);
    // dirB does not exist yet
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, true);

    // create dirB in UFS
    Assert.assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").mkdirs());

    // 'ONCE' still should not load the metadata
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false);

    // load metadata for dirB with 'ALWAYS'
    checkGetStatus("/mnt/dir1/dirA/dirB",
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always), true, true);

    // 'ONCE' should now load the metadata
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, true);
  }

  @Test
  public void loadMetadataOnceAfterUfsDelete() throws Exception {
    GetStatusOptions options =
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Once);
    // create dirB in UFS
    Assert.assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").mkdirs());

    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false);

    // delete dirB in UFS
    Assert.assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").delete());

    // 'ONCE' should not be affected if UFS is changed
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false);

    // force load metadata with 'ALWAYS'
    checkGetStatus("/mnt/dir1/dirA/dirB",
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always), false, true);

    // 'ONCE' should still not load metadata, since the ancestor is absent
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false);
  }

  @Test
  public void loadAlwaysConfiguration() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.Always.toString());
    GetStatusOptions options = GetStatusOptions.defaults();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
  }

  @Test
  public void loadOnceConfiguration() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.Once.toString());
    GetStatusOptions options = GetStatusOptions.defaults();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
  }

  @Test
  public void loadNeverConfiguration() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.Never.toString());
    GetStatusOptions options = GetStatusOptions.defaults();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false);
  }

  @Test
  public void loadRecursive() throws Exception {
    Configuration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.Once.toString());
    ListStatusOptions options = ListStatusOptions.defaults().setRecursive(true);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        new File(mLocalUfsPath + "/dir" + i + "/dir" + j + "/").mkdirs();
        FileWriter fileWriter = new FileWriter(mLocalUfsPath
            + "/dir" + i + "/dir" + j + "/" + "file");
        fileWriter.write("test" + i);
        fileWriter.close();
      }
    }
    long startMs = CommonUtils.getCurrentMs();
    List<URIStatus> list = mFileSystem.listStatus(new AlluxioURI("/mnt"), options);
    long durationMs = CommonUtils.getCurrentMs() - startMs;
    // 25 files, 25 level 2 dirs, 5 level 1 dirs, 1 file and 1 dir created in before
    Assert.assertEquals(25 * 2 + 5 + 2, list.size());

    // Should load metadata once, in one recursive call
    Assert.assertTrue("Expected to be between one and two SLEEP_MS. actual duration (ms): "
            + durationMs, durationMs >= LONG_SLEEP_MS && durationMs <= 2 * LONG_SLEEP_MS);
  }

  /**
   * Checks the get status call with the specified parameters and expectations.
   *
   * @param path the path to get the status for
   * @param options the options for the get status call
   * @param expectExists if true, the path should exist
   * @param expectLoadFromUfs if true, the get status call will load from ufs
   */
  private void checkGetStatus(final String path, GetStatusOptions options, boolean expectExists,
      boolean expectLoadFromUfs)
      throws Exception {
    long startMs = CommonUtils.getCurrentMs();
    try {
      mFileSystem.getStatus(new AlluxioURI(path), options);
      if (!expectExists) {
        Assert.fail("Path is not expected to exist: " + path);
      }
    } catch (FileDoesNotExistException e) {
      if (expectExists) {
        throw e;
      }
    }
    long durationMs = CommonUtils.getCurrentMs() - startMs;
    if (expectLoadFromUfs) {
      Assert.assertTrue("Expected to be slow (ufs load). actual duration (ms): " + durationMs,
          durationMs >= SLEEP_MS);
    } else {
      Assert.assertTrue("Expected to be fast (no ufs load). actual duration (ms): " + durationMs,
          durationMs < SLEEP_MS / 2);
    }

    if (!expectExists && expectLoadFromUfs) {
      // The metadata is loaded from Ufs, but the path does not exist, so it will be added to the
      // absent cache. Wait until the path shows up in the absent cache.
      UfsAbsentPathCache cache = getUfsAbsentPathCache();
      CommonUtils.waitFor("path (" + path + ") to be added to absent cache",
          () -> cache.isAbsent(new AlluxioURI(path)),
          WaitForOptions.defaults().setTimeoutMs(60000));
    }

    if (expectExists && expectLoadFromUfs) {
      // The metadata is loaded from Ufs, and the path exists, so it will be removed from the
      // absent cache. Wait until the path is removed.
      UfsAbsentPathCache cache = getUfsAbsentPathCache();
      CommonUtils.waitFor("path (" + path + ") to be removed from absent cache", () -> {
        if (cache.isAbsent(new AlluxioURI(path))) {
          return false;
        }
        return true;
      }, WaitForOptions.defaults().setTimeoutMs(60000));
    }
  }

  private UfsAbsentPathCache getUfsAbsentPathCache() {
    FileSystemMaster master = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(FileSystemMaster.class);
    return Whitebox.getInternalState(master, "mUfsAbsentPathCache");
  }
}

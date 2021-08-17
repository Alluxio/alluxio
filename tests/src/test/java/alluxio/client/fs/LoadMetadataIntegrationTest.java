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
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.client.file.FileOutStream;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.wire.LoadMetadataType;

import com.google.protobuf.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Tests the loading of metadata and the available options.
 */
public class LoadMetadataIntegrationTest extends BaseIntegrationTest {
  private static final long SLEEP_MS = Constants.SECOND_MS / 2;
  private static final int EXTRA_DIR_FILES = 6;

  private static final FileSystemMasterCommonPOptions PSYNC_ALWAYS =
      FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build();

  private FileSystem mFileSystem;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private String mLocalUfsPath;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test",
      ServerConfiguration.global());

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_METADATA_SYNC_UFS_PREFETCH_POOL_SIZE, 2).build();

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions()
              .setGetStatusMs(SLEEP_MS)
              .setExistsMs(SLEEP_MS)
              .setListStatusMs(SLEEP_MS)
              .setListStatusWithOptionsMs(SLEEP_MS)));

  @Before
  public void before() throws Exception {
    mLocalUfsPath = mTempFolder.getRoot().getAbsolutePath();
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
    mFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI("sleep://" + mLocalUfsPath));

    new File(mLocalUfsPath + "/dir1/dirA/").mkdirs();
    FileWriter fileWriter = new FileWriter(mLocalUfsPath + "/dir1/dirA/file");
    fileWriter.write("test");
    fileWriter.close();
    FileOutStream stream = mFileSystem.createFile(new AlluxioURI("/mnt/mustcache/dir1/dir2/file1"),
        CreateFilePOptions.newBuilder().setRecursive(true).setWriteType(WritePType.MUST_CACHE)
            .build());
    stream.write("test".getBytes(StandardCharsets.UTF_8));
    stream.close();
  }

  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  @Test
  public void symLink() throws Exception {
    Path target = Paths.get(mLocalUfsPath + "/dir1/dirA/file");
    Path link = Paths.get(mLocalUfsPath + "/symlink");
    Files.createSymbolicLink(link, target);

    Path target2 = Paths.get(mLocalUfsPath + "/dir1/dirA/");
    Path link2 = Paths.get(mLocalUfsPath + "/symlinkDir");
    Files.createSymbolicLink(link2, target2);

    ListStatusPOptions options =
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();

    List<URIStatus> statusList = checkListStatus("/mnt/", options, true, true, 3);
    assertEquals(4, statusList.size());
    assertTrue(statusList.stream().anyMatch(x -> x.getName().equals("symlink")));
    assertTrue(statusList.stream().anyMatch(x -> x.getName().equals("symlinkDir")));

    if (Files.exists(link)) {
      Files.delete(link);
    }
    if (Files.exists(link2)) {
      Files.delete(link2);
    }
  }

  @Test
  public void absentCacheListStatus() throws Exception {
    ListStatusPOptions options =
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    // The first time, it needs status of /dir1, /dir1/dirA to create the dirs in Alluxio
    // and the file itself
    checkListStatus("/mnt/dir1/dirA/file", options, true, true, 3);
    // The second time, it only needs to sync the file, so 1 access
    checkListStatus("/mnt/dir1/dirA/file", options, true, true, 0);
    // The first time, it needs status of /dir1, /dir1/dirA to create the dirs in Alluxio
    // and the file itself
    checkListStatus("/mnt/dir1/dirDNE", options, false, false, 1);
    // The second time, it only needs to sync the file, so 1 access
    checkListStatus("/mnt/dir1/dirDNE/dirDNE2/file1", options, false, false, 0);
  }

  @Test
  public void absentCacheGetStatus() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
        ).build();
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/dirDNE2/file2", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/dirDNE2", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/fileDNE", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dirB/fileDNE", options, false, false, 0);
    GetStatusPOptions optionsAlways =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
        ).build();
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", optionsAlways, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", optionsAlways, false, false, 1);
  }

  @Test
  public void absentCacheMustCache() throws Exception {
    ListStatusPOptions options =
        ListStatusPOptions.newBuilder().setRecursive(true)
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    GetStatusPOptions getStatusOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    // Each of these calls should only check one level of the directory,
    // because all the sub directories should be in the absent cache.
    checkListStatus("/mnt/mustcache/dir1/dir2", options, true, false, 1);
    checkListStatus("/mnt/mustcache/dir1", options, true, false, 1);
    checkListStatus("/mnt/mustcache", options, true, false, 1);
  }

  @Test
  public void repeatedDirectorySync() throws Exception {
    ListStatusPOptions options =
        ListStatusPOptions.newBuilder().setRecursive(true)
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    checkListStatus("/mnt/dir1/", options, true, true, 3);
    checkListStatus("/mnt/dir1/", options, true, true, 0);
  }

  @Test
  public void repeatedChildrenSync() throws Exception {
    ListStatusPOptions listOptions =
        ListStatusPOptions.newBuilder().setRecursive(true)
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    GetStatusPOptions getOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    // This can be improved
    checkGetStatus("/mnt/dir1/dirA/file", getOptions, true, true, 3);
    checkListStatus("/mnt/dir1/", listOptions, true, true, 3);
  }

  @Test
  public void syncOverrideLoadMetadata() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)
            ).build();
    // The first time, it needs status of /dir1, /dir1/dirA to create the dirs in Alluxio
    // and the file itself
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true, 3);
    // The second time, it only needs to sync the file, so 1 access
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true, 1);
  }

  @Test
  public void loadMetadataListDir() throws Exception {
    ListStatusPOptions listOptions =
        ListStatusPOptions.newBuilder().setRecursive(false)
            .setLoadMetadataType(LoadMetadataPType.ONCE)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1)
            ).build();
    checkListStatus("/mnt/", listOptions, true, true, 3);
  }

  @Test
  public void loadMetadataAlways() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true, 3);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, false, 1);
  }

  @Test
  public void loadMetadataNever() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/file", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/fileDNE3", options, false, true, 0);
  }

  @Test
  public void loadMetadataOnce() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).build();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/fileDNE2", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/file", options, true, true, 3);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir1", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir1/file1", options, false, false, 0);
    checkGetStatus("/mnt/dir1/dirA/dirDNE/dir2", options, false, false, 0);
  }

  @Test
  public void loadMetadataOnceAfterUfsCreate() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).build();
    // dirB does not exist yet
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 1);

    // create dirB in UFS
    assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").mkdirs());

    // 'ONCE' still should not load the metadata
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 0);

    // load metadata for dirB with 'ALWAYS'
    checkGetStatus("/mnt/dir1/dirA/dirB",
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build(), true,
        true, 3);

    // 'ONCE' should now load the metadata, but will be handled by the absent cache,
    // so no need to go to the ufs
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 0);
  }

  @Test
  public void listStatusLoadsFile() throws Exception {
    GetStatusPOptions getStatusOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).build();
    ListStatusPOptions lsOptions =
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)
            .setCommonOptions(PSYNC_ALWAYS).build();
    // load metadata for dirA with 'ALWAYS'
    checkListStatus("/mnt/dir1/dirA", lsOptions, true,
        true, 3);

    // the file should already be loaded
    checkGetStatus("/mnt/dir1/dirA/file", getStatusOptions, true,
        true, 0);
  }

  @Test
  public void getStatusDoesNotLoadFile() throws Exception {
    GetStatusPOptions getStatusOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).build();
    ListStatusPOptions lsOptions =
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)
            .setCommonOptions(PSYNC_ALWAYS).build();
    // load metadata for dirA with 'ALWAYS'
    checkGetStatus("/mnt/dir1/dirA", getStatusOptions, true,
        true, 2);

    // the file should NOT already be loaded
    checkGetStatus("/mnt/dir1/dirA/file", getStatusOptions, true,
        true, 1);
  }

  @Test
  public void loadMetadataOnceAfterUfsDelete() throws Exception {
    GetStatusPOptions options =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).build();
    // create dirB in UFS
    assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").mkdirs());

    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 0);

    // delete dirB in UFS
    assertTrue(new File(mLocalUfsPath + "/dir1/dirA/dirB").delete());

    // 'ONCE' should not be affected if UFS is changed
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 0);

    // force load metadata with 'ALWAYS'
    checkGetStatus("/mnt/dir1/dirA/dirB",
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build(), false,
        false, 1);

    // 'ONCE' should still not load metadata, since the ancestor is absent
    checkGetStatus("/mnt/dir1/dirA/dirB/file", options, false, false, 0);
  }

  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "ALWAYS"
      })
  @Test
  public void loadAlwaysConfiguration() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.getDefaultInstance();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
  }

  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "ONCE"
      })
  @Test
  public void loadOnceConfiguration() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.getDefaultInstance();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 1);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, false, 0);
  }

  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.USER_FILE_METADATA_LOAD_TYPE, "NEVER"
      })
  @Test
  public void loadNeverConfiguration() throws Exception {
    GetStatusPOptions options = GetStatusPOptions.getDefaultInstance();
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true, 0);
    checkGetStatus("/mnt/dir1/dirA/fileDNE1", options, false, true, 0);
  }

  @Test
  public void loadRecursive() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE,
        LoadMetadataType.ONCE.toString());
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    int createdInodes = createUfsFiles(5);
    List<URIStatus> list = mFileSystem.listStatus(new AlluxioURI("/mnt"), options);
    // 25 files, 25 level 2 dirs, 5 level 1 dirs, 1 file and 1 dir created in before
    // 4 directories/files in must cache
    assertEquals(createdInodes + EXTRA_DIR_FILES, list.size());
  }

  @Test
  public void testNoTtlOnLoadedFiles() throws Exception {
    int created = createUfsFiles(2);
    ServerConfiguration.set(PropertyKey.USER_FILE_METADATA_LOAD_TYPE,
        LoadMetadataType.ONCE.toString());
    ServerConfiguration.set(PropertyKey.USER_FILE_CREATE_TTL, "11000");
    ServerConfiguration.set(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.FREE.toString());
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder()
                .setTtl(10000)
                .setTtlAction(TtlAction.FREE)
                .build())
        .build();
    List<URIStatus> list = mFileSystem.listStatus(new AlluxioURI("/mnt"), options);
    assertEquals(created + EXTRA_DIR_FILES, list.size());
    list.forEach(stat -> {
      assertEquals(-1, stat.getTtl());
    });
  }

  public int createUfsFiles(int loops) throws Exception {
    int count = 0;
    for (int i = 0; i < loops; i++) {
      new File(mLocalUfsPath + "/dir" + i + "/").mkdirs();
      count += 1; // 1 dirs
      for (int j = 0; j < loops; j++) {
        new File(mLocalUfsPath + "/dir" + i + "/dir" + j + "/").mkdirs();
        FileWriter fileWriter = new FileWriter(mLocalUfsPath
            + "/dir" + i + "/dir" + j + "/" + "file");
        fileWriter.write("test" + i);
        fileWriter.close();
        count += 2; // 1 dir and 1 file
      }
    }
    return count;
  }

  /**
   * This test makes sure that we can sync files deeply nested from a UFS without issues.
   *
   * Previous versions of the code caused a deadlock, which is why this test was added.
   */
  @Test(timeout = 20000)
  public void loadNonexistentSubpath() throws Exception {
    String rootUfs = mFileSystem.getMountTable().get("/").getUfsUri();
    String subdirPath = "/i/dont/exist/in/alluxio";
    String ufsPath = PathUtils.concatPath(rootUfs, subdirPath);
    assertTrue(new File(ufsPath).mkdirs());
    String filepath = PathUtils.concatPath(ufsPath, "a");
    String data = "testtesttest";
    try (FileWriter w = new FileWriter(filepath)) {
      w.write(data);
    }
    String alluxioPath = PathUtils.concatPath(subdirPath, "a");
    URIStatus s = mFileSystem.getStatus(new AlluxioURI(alluxioPath), GetStatusPOptions.newBuilder()
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())
        .build());
    assertFalse(s.isFolder());
    assertEquals(data.length(), s.getLength());
    assertEquals("a", s.getName());
    assertTrue(s.isPersisted());
  }

  @FunctionalInterface
  public interface CheckedBiFunction<T, U, R> {
    R apply(T t, U u) throws Exception;
  }

  /**
   * Checks the get status call with the specified parameters and expectations.
   *
   * @param path the path to get the status for
   * @param options the options for the get status call
   * @param expectExistsAlluxio if true, the path should exist in Alluxio
   * @param expectExistsUfs if true, the path should exist in UFS and be removed from absent cache
   * @param expectedAccesses the number of expected UFS Accesses
   *
   * @return URIStatus the URIStatus that is returned from the getStatus call
   */
  private URIStatus checkGetStatus(final String path, GetStatusPOptions options,
      boolean expectExistsAlluxio, boolean expectExistsUfs, int expectedAccesses)
      throws Exception {
    return (URIStatus) checkFunctionCall(path,
        (String statusPath, Message statusOption) -> mFileSystem.getStatus(
            new AlluxioURI(statusPath), (GetStatusPOptions) statusOption),
        options, expectExistsAlluxio, expectExistsUfs, expectedAccesses);
  }

  private List<URIStatus> checkListStatus(final String path, ListStatusPOptions options,
      boolean expectExistsAlluxio, boolean expectExistsUfs, int expectedAccesses)
      throws Exception {
    return (List<URIStatus>) checkFunctionCall(path,
        (String statusPath, Message statusOption) -> mFileSystem.listStatus(
            new AlluxioURI(statusPath), (ListStatusPOptions) statusOption),
        options, expectExistsAlluxio, expectExistsUfs, expectedAccesses);
  }

  private Object checkFunctionCall(final String path,
      CheckedBiFunction<String, Message, Object> function,
      Message options, boolean expectExistsAlluxio, boolean expectExistsUfs,
      int expectedAccesses) throws Exception {
    long startMs = CommonUtils.getCurrentMs();
    Object result = null;
    try {
      result = function.apply(path, options);
      UfsAbsentPathCache cache = getUfsAbsentPathCache();
      if (!expectExistsAlluxio) {
        Assert.fail("Path is not expected to exist: " + path);
      }
    } catch (FileDoesNotExistException e) {
      if (expectExistsAlluxio) {
        throw e;
      }
    }
    long durationMs = CommonUtils.getCurrentMs() - startMs;
    assertTrue("Expected to be take between " + expectedAccesses * SLEEP_MS
            + " and " + (expectedAccesses + 0.5) * SLEEP_MS
            + ". actual duration (ms): " + durationMs,
        durationMs >= expectedAccesses * SLEEP_MS
            && durationMs < (expectedAccesses + 0.5) * SLEEP_MS);

    if (!expectExistsUfs) {
      // The metadata is loaded from Ufs, but the path does not exist, so it will be added to the
      // absent cache. Wait until the path shows up in the absent cache.
      UfsAbsentPathCache cache = getUfsAbsentPathCache();
      try {
        CommonUtils.waitFor("path (" + path + ") to be added to absent cache",
            () -> cache.isAbsentSince(new AlluxioURI(path), UfsAbsentPathCache.ALWAYS),
            WaitForOptions.defaults().setTimeoutMs(60000));
      } catch (TimeoutException e) {
        fail("Absent Path Cache addition timed out");
      }
    }

    if (expectExistsUfs) {
      // The metadata is loaded from Ufs, and the path exists, so it will be removed from the
      // absent cache. Wait until the path is removed.
      UfsAbsentPathCache cache = getUfsAbsentPathCache();
      try {
        CommonUtils.waitFor("path (" + path + ") to be removed from absent cache", () -> {
          if (cache.isAbsentSince(new AlluxioURI(path), UfsAbsentPathCache.ALWAYS)) {
            return false;
          }
          return true;
        }, WaitForOptions.defaults().setTimeoutMs(60000));
      } catch (TimeoutException e) {
        fail("Absent Path Cache removal timed out");
      }
    }
    return result;
  }

  private UfsAbsentPathCache getUfsAbsentPathCache() {
    FileSystemMaster master = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getMasterProcess().getMaster(FileSystemMaster.class);
    return Whitebox.getInternalState(master, "mUfsAbsentPathCache");
  }
}

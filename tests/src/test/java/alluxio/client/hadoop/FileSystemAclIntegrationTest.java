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

package alluxio.client.hadoop;

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.hadoop.HadoopClientTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.hadoop.FileSystem;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Integration tests for {@link FileSystem#setOwner(Path, String, String)} and
 * {@link FileSystem#setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)}.
 */
public final class FileSystemAclIntegrationTest extends BaseIntegrationTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName())
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH")
          .build();
  private static String sUfsRoot;
  private static UnderFileSystem sUfs;
  private static org.apache.hadoop.fs.FileSystem sTFS;

  private static void create(org.apache.hadoop.fs.FileSystem fs, Path path) throws IOException {
    FSDataOutputStream o = fs.create(path);
    o.writeBytes("Test Bytes");
    o.close();
  }

  /**
   * Deletes files in the given filesystem.
   *
   * @param fs given filesystem
   */
  public static void cleanup(org.apache.hadoop.fs.FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path("/"));
    for (FileStatus f : statuses) {
      fs.delete(f.getPath(), true);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());
    conf = HadoopConfigurationUtils.mergeAlluxioConfiguration(conf, ServerConfiguration.global());

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterURI());

    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, HadoopConfigurationUtils
        .mergeAlluxioConfiguration(conf, ServerConfiguration.global()));
    sUfsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    sUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
  }

  @After
  public void cleanupTFS() throws Exception {
    cleanup(sTFS);
  }

  @Test
  public void createFileWithPermission() throws Exception {
    List<Integer> permissionValues =
        Lists.newArrayList(0111, 0222, 0333, 0444, 0555, 0666, 0777, 0755, 0733, 0644, 0533, 0511);
    for (int value : permissionValues) {
      Path file = new Path("/createfile" + value);
      FsPermission permission = FsPermission.createImmutable((short) value);
      FSDataOutputStream o = sTFS.create(file, permission, false /* ignored */, 10 /* ignored */,
          (short) 1 /* ignored */, 512 /* ignored */, null /* ignored */);
      o.writeBytes("Test Bytes");
      o.close();
      FileStatus fs = sTFS.getFileStatus(file);
      Assert.assertEquals(permission, fs.getPermission());
    }
  }

  @Test
  public void mkdirsWithPermission() throws Exception {
    List<Integer> permissionValues =
        Lists.newArrayList(0111, 0222, 0333, 0444, 0555, 0666, 0777, 0755, 0733, 0644, 0533, 0511);
    for (int value : permissionValues) {
      Path dir = new Path("/createDir" + value);
      FsPermission permission = FsPermission.createImmutable((short) value);
      sTFS.mkdirs(dir, permission);
      FileStatus fs = sTFS.getFileStatus(dir);
      Assert.assertEquals(permission, fs.getPermission());
    }
  }

  /**
   * Test for {@link FileSystem#setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)}.
   * It will test changing the permission of file using TFS.
   */
  @Test
  public void chmod() throws Exception {
    Path fileA = new Path("/chmodfileA");

    create(sTFS, fileA);
    FileStatus fs = sTFS.getFileStatus(fileA);
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, fileA)));

    if (UnderFileSystemUtils.isHdfs(sUfs) && HadoopClientTestUtils.isHadoop1x()) {
      // If the UFS is hadoop 1.0, the org.apache.hadoop.fs.FileSystem.create uses default
      // permission option 0777.
      Assert.assertEquals((short) 0777, fs.getPermission().toShort());
    } else {
      // Default permission should be 0644.
      Assert.assertEquals((short) 0644, fs.getPermission().toShort());
    }

    sTFS.setPermission(fileA, FsPermission.createImmutable((short) 0755));
    Assert.assertEquals((short) 0755, sTFS.getFileStatus(fileA).getPermission().toShort());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with local UFS. It will test only
   * changing the owner of file using TFS and propagate the change to UFS. Since the arbitrary
   * owner does not exist in the local UFS, the operation would fail.
   */
  @Test
  public void changeNonexistentOwnerForLocal() throws Exception {
    // Skip non-local UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs));

    Path fileA = new Path("/chownfileA-local");
    final String nonexistentOwner = "nonexistent-user1";
    final String nonexistentGroup = "nonexistent-group1";

    create(sTFS, fileA);

    FileStatus fs = sTFS.getFileStatus(fileA);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA)).getOwner());
    // Group can different because local FS user to group mapping can be different from that
    // in Alluxio.

    Assert.assertNotEquals(defaultOwner, nonexistentOwner);
    Assert.assertNotEquals(defaultGroup, nonexistentGroup);

    // Expect a IOException for not able to setOwner for UFS with invalid owner name.
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Could not setOwner for UFS file");
    sTFS.setOwner(fileA, nonexistentOwner, null);
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with local UFS. It will test only
   * changing the group of file using TFS and propagate the change to UFS. Since the arbitrary
   * group does not exist in the local UFS, the operation would fail.
   */
  @Test
  public void changeNonexistentGroupForLocal() throws Exception {
    // Skip non-local UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs));

    Path fileB = new Path("/chownfileB-local");
    final String nonexistentOwner = "nonexistent-user1";
    final String nonexistentGroup = "nonexistent-group1";

    create(sTFS, fileB);

    FileStatus fs = sTFS.getFileStatus(fileB);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileB)).getOwner());
    // Group can different because local FS user to group mapping can be different from that
    // in Alluxio.

    Assert.assertNotEquals(defaultOwner, nonexistentOwner);
    Assert.assertNotEquals(defaultGroup, nonexistentGroup);

    // Expect a IOException for not able to setOwner for UFS with invalid group name.
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Could not setOwner for UFS file");
    sTFS.setOwner(fileB, null, nonexistentGroup);
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with local UFS. It will test
   * changing both owner and group of file using TFS and propagate the change to UFS. Since the
   * arbitrary owner and group do not exist in the local UFS, the operation would fail.
   */
  @Test
  public void changeNonexistentOwnerAndGroupForLocal() throws Exception {
    // Skip non-local UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs));

    Path fileC = new Path("/chownfileC-local");
    final String nonexistentOwner = "nonexistent-user1";
    final String nonexistentGroup = "nonexistent-group1";

    create(sTFS, fileC);

    FileStatus fs = sTFS.getFileStatus(fileC);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileC)).getOwner());
    // Group can different because local FS user to group mapping can be different from that
    // in Alluxio.

    Assert.assertNotEquals(defaultOwner, nonexistentOwner);
    Assert.assertNotEquals(defaultGroup, nonexistentGroup);

    mThrown.expect(IOException.class);
    mThrown.expectMessage("Could not update owner");
    sTFS.setOwner(fileC, nonexistentOwner, nonexistentGroup);
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with HDFS UFS. It will test only
   * changing the owner of file using TFS and propagate the change to UFS.
   */
  @Test
  public void changeNonexistentOwnerForHdfs() throws Exception {
    // Skip non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isHdfs(sUfs));

    Path fileA = new Path("/chownfileA-hdfs");
    final String testOwner = "test-user1";
    final String testGroup = "test-group1";

    create(sTFS, fileA);

    FileStatus fs = sTFS.getFileStatus(fileA);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA)).getOwner());
    // Group can different because HDFS user to group mapping can be different from that in Alluxio.

    Assert.assertNotEquals(defaultOwner, testOwner);
    Assert.assertNotEquals(defaultGroup, testGroup);

    // Expect a IOException for not able to setOwner for UFS with invalid owner name.
    sTFS.setOwner(fileA, testOwner, null);

    fs = sTFS.getFileStatus(fileA);
    Assert.assertEquals(testOwner, fs.getOwner());
    Assert.assertEquals(defaultGroup, fs.getGroup());
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertEquals(testOwner, ufsStatus.getOwner());
    Assert.assertEquals(defaultGroup, ufsStatus.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with HDFS UFS. It will test only
   * changing the group of file using TFS and propagate the change to UFS.
   */
  @Test
  public void changeNonexistentGroupForHdfs() throws Exception {
    // Skip non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isHdfs(sUfs));

    Path fileB = new Path("/chownfileB-hdfs");
    final String testOwner = "test-user1";
    final String testGroup = "test-group1";

    create(sTFS, fileB);

    FileStatus fs = sTFS.getFileStatus(fileB);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileB)).getOwner());
    // Group can different because HDFS user to group mapping can be different from that in Alluxio.

    Assert.assertNotEquals(defaultOwner, testOwner);
    Assert.assertNotEquals(defaultGroup, testGroup);

    sTFS.setOwner(fileB, null, testGroup);
    fs = sTFS.getFileStatus(fileB);
    Assert.assertEquals(defaultOwner, fs.getOwner());
    Assert.assertEquals(testGroup, fs.getGroup());
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileB));
    Assert.assertEquals(defaultOwner, ufsStatus.getOwner());
    Assert.assertEquals(testGroup, ufsStatus.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)} with HDFS UFS. It will test
   * changing both owner and group of file using TFS and propagate the change to UFS.
   */
  @Test
  public void changeNonexistentOwnerAndGroupForHdfs() throws Exception {
    // Skip non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isHdfs(sUfs));

    Path fileC = new Path("/chownfileC-hdfs");
    final String testOwner = "test-user1";
    final String testGroup = "test-group1";

    create(sTFS, fileC);

    FileStatus fs = sTFS.getFileStatus(fileC);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertEquals(defaultOwner,
        sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileC)).getOwner());
    // Group can different because HDFS user to group mapping can be different from that in Alluxio.

    Assert.assertNotEquals(defaultOwner, testOwner);
    Assert.assertNotEquals(defaultGroup, testGroup);

    sTFS.setOwner(fileC, testOwner, testGroup);
    fs = sTFS.getFileStatus(fileC);
    Assert.assertEquals(testOwner, fs.getOwner());
    Assert.assertEquals(testGroup, fs.getGroup());
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileC));
    Assert.assertEquals(testOwner, ufsStatus.getOwner());
    Assert.assertEquals(testGroup, ufsStatus.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)}. It will test both owner and group
   * are null.
   */
  @Test
  public void checkNullOwnerAndGroup() throws Exception {
    Path fileD = new Path("/chownfileD");

    create(sTFS, fileD);

    FileStatus fs = sTFS.getFileStatus(fileD);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    sTFS.setOwner(fileD, null, null);

    fs = sTFS.getFileStatus(fileD);
    Assert.assertEquals(defaultOwner, fs.getOwner());
    Assert.assertEquals(defaultGroup, fs.getGroup());
  }

  /**
   * Tests the directory permission propagation to UFS.
   */
  @Test
  public void directoryPermissionForUfs() throws IOException {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs) || UnderFileSystemUtils.isHdfs(sUfs));

    Path dir = new Path("/root/directoryPermissionForUfsDir");
    sTFS.mkdirs(dir);

    FileStatus fs = sTFS.getFileStatus(dir);
    String defaultOwner = fs.getOwner();
    Short dirMode = fs.getPermission().toShort();
    FileStatus parentFs = sTFS.getFileStatus(dir.getParent());
    Short parentMode = parentFs.getPermission().toShort();

    UfsStatus ufsStatus = sUfs.getDirectoryStatus(PathUtils.concatPath(sUfsRoot, dir));
    Assert.assertEquals(defaultOwner, ufsStatus.getOwner());
    Assert.assertEquals((int) dirMode, (int) ufsStatus.getMode());
    Assert.assertEquals((int) parentMode,
        (int) sUfs.getDirectoryStatus(PathUtils.concatPath(sUfsRoot, dir.getParent())).getMode());

    short newMode = (short) 0755;
    FsPermission newPermission = new FsPermission(newMode);
    sTFS.setPermission(dir, newPermission);

    Assert.assertEquals((int) newMode,
        (int) sUfs.getDirectoryStatus(PathUtils.concatPath(sUfsRoot, dir)).getMode());
  }

  /**
   * Tests the parent directory permission when mkdirs recursively.
   */
  @Test
  public void parentDirectoryPermissionForUfs() throws IOException {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs) || UnderFileSystemUtils.isHdfs(sUfs));

    String path = "/root/parentDirectoryPermissionForUfsDir/parentDirectoryPermissionForUfsFile";
    Path fileA = new Path(path);
    Path dirA = fileA.getParent();
    sTFS.mkdirs(dirA);
    short parentMode = (short) 0700;
    FsPermission newPermission = new FsPermission(parentMode);
    sTFS.setPermission(dirA, newPermission);

    create(sTFS, fileA);

    Assert.assertEquals((int) parentMode,
        (int) sUfs.getDirectoryStatus(PathUtils.concatPath(sUfsRoot, dirA)).getMode());

    // Rename from dirA to dirB, file and its parent permission should be in sync with the source
    // dirA.
    Path fileB = new Path("/root/dirB/fileB");
    Path dirB = fileB.getParent();
    sTFS.rename(dirA, dirB);
    Assert.assertEquals((int) parentMode,
        (int) sUfs.getDirectoryStatus(PathUtils.concatPath(sUfsRoot, fileB.getParent())).getMode());
  }

  /**
   * Tests the loaded file metadata from UFS having the same mode as that in the UFS.
   */
  @Test
  public void loadFileMetadataMode() throws Exception {
    // Skip non-local and non-HDFS-2 UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs)
        || (UnderFileSystemUtils.isHdfs(sUfs) && HadoopClientTestUtils.isHadoop2x()));

    List<Integer> permissionValues =
        Lists.newArrayList(0111, 0222, 0333, 0444, 0555, 0666, 0777, 0755, 0733, 0644, 0533, 0511);

    for (int value : permissionValues) {
      Path file = new Path("/loadFileMetadataMode" + value);
      sTFS.delete(file, false);
      // Create a file directly in UFS and set the corresponding mode.
      String ufsPath = PathUtils.concatPath(sUfsRoot, file);
      sUfs.create(ufsPath, CreateOptions.defaults(ServerConfiguration.global()).setOwner("testuser")
          .setGroup("testgroup").setMode(new Mode((short) value))).close();
      Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, file)));
      // Check the mode is consistent in Alluxio namespace once it's loaded from UFS to Alluxio.
      Assert.assertEquals(new Mode((short) value).toString(),
          new Mode(sTFS.getFileStatus(file).getPermission().toShort()).toString());
    }
  }

  /**
   * Tests the loaded directory metadata from UFS having the same mode as that in the UFS.
   */
  @Test
  public void loadDirMetadataMode() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(sUfs) || UnderFileSystemUtils.isHdfs(sUfs));

    List<Integer> permissionValues =
        Lists.newArrayList(0111, 0222, 0333, 0444, 0555, 0666, 0777, 0755, 0733, 0644, 0533, 0511);

    for (int value : permissionValues) {
      Path dir = new Path("/loadDirMetadataMode" + value + "/");
      sTFS.delete(dir, true);
      // Create a directory directly in UFS and set the corresponding mode.
      String ufsPath = PathUtils.concatPath(sUfsRoot, dir);
      sUfs.mkdirs(ufsPath,
          MkdirsOptions.defaults(ServerConfiguration.global()).setCreateParent(false)
              .setOwner("testuser").setGroup("testgroup").setMode(new Mode((short) value)));
      Assert.assertTrue(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, dir)));
      // Check the mode is consistent in Alluxio namespace once it's loaded from UFS to Alluxio.
      Assert.assertEquals(new Mode((short) value).toString(),
          new Mode(sTFS.getFileStatus(dir).getPermission().toShort()).toString());
    }
  }

  @Test
  public void s3GetPermission() throws Exception {
    Assume.assumeTrue(UnderFileSystemUtils.isS3(sUfs));

    ServerConfiguration.unset(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING);
    Path fileA = new Path("/s3GetPermissionFile");
    create(sTFS, fileA);
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, fileA)));

    // Without providing "alluxio.underfs.s3.canonical.owner.id.to.username.mapping", the default
    // display name of the S3 owner account is NOT empty.
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertNotEquals("", ufsStatus.getOwner());
    Assert.assertNotEquals("", ufsStatus.getGroup());
    Assert.assertEquals((short) 0700, ufsStatus.getMode());
  }

  @Test
  public void gcsGetPermission() throws Exception {
    Assume.assumeTrue(UnderFileSystemUtils.isGcs(sUfs));

    ServerConfiguration.unset(PropertyKey.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING);
    Path fileA = new Path("/gcsGetPermissionFile");
    create(sTFS, fileA);
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, fileA)));

    // Without providing "alluxio.underfs.gcs.owner.id.to.username.mapping", the default
    // display name of the GCS owner account is empty. The owner will be the GCS account id, which
    // is not empty.
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertNotEquals("", ufsStatus.getOwner());
    Assert.assertNotEquals("", ufsStatus.getGroup());
    Assert.assertEquals((short) 0700, ufsStatus.getMode());
  }

  @Test
  public void swiftGetPermission() throws Exception {
    Assume.assumeTrue(UnderFileSystemUtils.isSwift(sUfs));

    Path fileA = new Path("/swiftGetPermissionFile");
    create(sTFS, fileA);
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, fileA)));

    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertNotEquals("", ufsStatus.getOwner());
    Assert.assertNotEquals("", ufsStatus.getGroup());
    Assert.assertEquals((short) 0700, ufsStatus.getMode());
  }

  @Test
  public void ossGetPermission() throws Exception {
    Assume.assumeTrue(UnderFileSystemUtils.isOss(sUfs));

    Path fileA = new Path("/objectfileA");
    create(sTFS, fileA);
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, fileA)));

    // Verify the owner, group and permission of OSS UFS is not supported and thus returns default
    // values.
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertNotEquals("", ufsStatus.getOwner());
    Assert.assertNotEquals("", ufsStatus.getGroup());
    Assert.assertEquals(Constants.DEFAULT_FILE_SYSTEM_MODE, ufsStatus.getMode());
  }

  @Test
  public void objectStoreSetOwner() throws Exception {
    Assume.assumeTrue(sUfs.isObjectStorage());

    Path fileA = new Path("/objectfileA");
    final String newOwner = "new-user1";
    final String newGroup = "new-group1";
    create(sTFS, fileA);

    // Set owner to Alluxio files that are persisted in UFS will NOT propagate to underlying object.
    sTFS.setOwner(fileA, newOwner, newGroup);
    UfsStatus ufsStatus = sUfs.getFileStatus(PathUtils.concatPath(sUfsRoot, fileA));
    Assert.assertNotEquals("", ufsStatus.getOwner());
    Assert.assertNotEquals("", ufsStatus.getGroup());
  }
}

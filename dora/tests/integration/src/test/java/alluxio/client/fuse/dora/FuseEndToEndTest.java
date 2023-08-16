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

package alluxio.client.fuse.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.options.FuseOptions;
import alluxio.jnifuse.LibFuse;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.underfs.s3a.S3AUnderFileSystemFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

/**
 * Isolation tests for {@link alluxio.fuse.AlluxioJniFuseFileSystem} with local UFS.
 * This test covers the basic file system metadata operations.
 */
@Ignore("Failed to unmount because of Permission Denied")
public class FuseEndToEndTest {
  private static final String TEST_S3A_PATH_CONF = "alluxio.test.s3a.path";
  private static final String MOUNT_POINT = AlluxioTestDirectory
      .createTemporaryDirectory("ufs").toString();

  @BeforeClass
  public static void beforeClass() throws Exception {
    String s3Path = System.getProperty(TEST_S3A_PATH_CONF);
    String ufs;
    if (s3Path != null) { // test against S3
      ufs = new AlluxioURI(s3Path).join(UUID.randomUUID().toString()).toString();
      UnderFileSystemFactoryRegistry.register(new S3AUnderFileSystemFactory());
    } else { // test against local
      ufs = AlluxioTestDirectory.createTemporaryDirectory("ufs").toString();
      UnderFileSystemFactoryRegistry.register(new LocalUnderFileSystemFactory());
    }
    InstancedConfiguration conf = Configuration.copyGlobal();
    conf.set(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT, Source.RUNTIME);
    FileSystemContext context = FileSystemContext.create(ClientContext.create(conf));
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
    UfsFileSystemOptions ufsOptions =  new UfsFileSystemOptions(ufs);
    FileSystem fileSystem = new UfsBaseFileSystem(context, ufsOptions);
    final FileSystemOptions fileSystemOptions =
        FileSystemOptions.Builder
            .fromConf(context.getClusterConf())
            .setUfsFileSystemOptions(ufsOptions)
            .build();
    AlluxioJniFuseFileSystem fuseFileSystem = new AlluxioJniFuseFileSystem(context, fileSystem,
        FuseOptions.Builder.fromConfig(Configuration.global())
            .setFileSystemOptions(fileSystemOptions)
            .setUpdateCheckEnabled(false)
            .build());
    fuseFileSystem.mount(false, false, new HashSet<>());
    if (!FuseUtils.waitForFuseMounted(MOUNT_POINT)) {
      FuseUtils.umountFromShellIfMounted(MOUNT_POINT);
      fail("Could not setup FUSE mount point");
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    FuseUtils.umountFromShellIfMounted(MOUNT_POINT);
  }

  @Test
  public void createDeleteEmptyFile() throws Exception {
    String testFile = Paths.get(MOUNT_POINT, "/createDeleteEmptyFile").toString();
    File file = new File(testFile);
    assertFalse(file.exists());
    new FileOutputStream(testFile).close();
    assertTrue(file.exists());
    assertEquals(0, file.length());
    assertTrue(file.isFile());
    assertTrue(file.delete());
    assertFalse(file.exists());
  }

  @Test
  public void createDeleteDirectory() {
    String testDir = Paths.get(MOUNT_POINT, "/createDeleteDirectory").toString();
    File dir = new File(testDir);
    assertFalse(dir.exists());
    assertTrue(dir.mkdir());
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());
    assertTrue(dir.delete());
    assertFalse(dir.exists());
  }

  @Test
  public void writeThenRead() throws Exception {
    String testFile = Paths.get(MOUNT_POINT, "/writeThenRead").toString();
    byte[] content = "Alluxio Fuse Test File Content".getBytes();
    File file = new File(testFile);
    assertFalse(file.exists());
    try (FileOutputStream outputStream = new FileOutputStream(testFile)) {
      outputStream.write(content);
    }
    assertTrue(file.exists());
    assertTrue(file.isFile());
    assertEquals(content.length, file.length());
    try (FileInputStream inputStream = new FileInputStream(testFile)) {
      byte[] res = new byte[content.length];
      assertEquals(content.length, inputStream.read(res));
      assertEquals(Arrays.toString(content), Arrays.toString(res));
    }
    assertTrue(file.delete());
    assertFalse(file.exists());
  }

  @Test
  public void listDirectory() throws Exception {
    String testDir = Paths.get(MOUNT_POINT, "/listDirectory").toString();
    File dir = new File(testDir);
    assertFalse(dir.exists());
    assertTrue(dir.mkdir());
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());
    String testFile1 = Paths.get(testDir, "/file1").toString();
    String testFile2 = Paths.get(testDir, "/file2").toString();
    new FileOutputStream(testFile1).close();
    new FileOutputStream(testFile2).close();
    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(2, files.length);
  }

  @Test
  @Ignore("Throwing core dump, need to debug")
  public void rename() throws Exception {
    String srcFile = Paths.get(MOUNT_POINT, "/renameSrc").toString();
    String dstFile = Paths.get(MOUNT_POINT, "/renameDst").toString();
    File file = new File(srcFile);
    new FileOutputStream(srcFile).close();
    assertTrue(file.renameTo(new File(dstFile)));
  }
}

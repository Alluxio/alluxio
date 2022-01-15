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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.hadoop.FileSystem;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Integration tests for {@link FileSystem#rename(Path, Path)}.
 */
// TODO(jiri): Test persisting rename operations to UFS.
public final class FileSystemRenameIntegrationTest extends BaseIntegrationTest {
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH").build();
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

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterURI());

    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, HadoopConfigurationUtils
        .mergeAlluxioConfiguration(conf, ServerConfiguration.global()));
    sUfsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    sUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
  }

  @Test
  public void basicRenameTest1() throws Exception {
    // Rename /fileA to /fileB
    Path fileA = new Path("/fileA");
    Path fileB = new Path("/fileB");

    create(sTFS, fileA);

    Assert.assertTrue(sTFS.rename(fileA, fileB));

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(fileB));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileB")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(fileB));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileB")));
  }

  @Test
  public void basicRenameTest2() throws Exception {
    // Rename /fileA to /dirA/fileA
    Path fileA = new Path("/fileA");
    Path dirA = new Path("/dirA");
    Path finalDst = new Path("/dirA/fileA");

    create(sTFS, fileA);
    sTFS.mkdirs(dirA);

    Assert.assertTrue(sTFS.rename(fileA, finalDst));

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirA));
    Assert.assertTrue(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirA", "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
  }

  @Test
  public void basicRenameTest3() throws Exception {
    // Rename /fileA to /dirA/fileA without specifying the full path
    Path fileA = new Path("/fileA");
    Path dirA = new Path("/dirA");
    Path finalDst = new Path("/dirA/fileA");

    create(sTFS, fileA);
    sTFS.mkdirs(dirA);

    Assert.assertTrue(sTFS.rename(fileA, dirA));

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirA));
    Assert.assertTrue(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirA", "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
  }

  @Test
  public void basicRenameTest4() throws Exception {
    // Rename /fileA to /fileA
    Path fileA = new Path("/fileA");

    create(sTFS, fileA);

    Assert.assertTrue(sTFS.rename(fileA, fileA));

    Assert.assertTrue(sTFS.exists(fileA));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
  }

  @Test
  public void basicRenameTest5() throws Exception {
    // Rename /fileA to /fileAfileA
    Path fileA = new Path("/fileA");
    Path finalDst = new Path("/fileAfileA");

    create(sTFS, fileA);

    Assert.assertTrue(sTFS.rename(fileA, finalDst));

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileAfileA")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileAfileA")));
  }

  @Test
  public void basicRenameTest6() throws Exception {
    // Rename /dirA to /dirB, /dirA/fileA should become /dirB/fileA
    Path dirA = new Path("/dirA");
    Path dirB = new Path("/dirB");
    Path fileA = new Path("/dirA/fileA");
    Path finalDst = new Path("/dirB/fileA");

    sTFS.mkdirs(dirA);
    create(sTFS, fileA);

    Assert.assertTrue(sTFS.rename(dirA, dirB));

    Assert.assertFalse(sTFS.exists(dirA));
    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirB));
    Assert.assertTrue(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirA", "fileA")));
    Assert.assertTrue(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirB")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirB", "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(dirB));
    Assert.assertFalse(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirB")));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirB", "fileA")));
  }

  @Test
  @Ignore
  // TODO(jiri): The test logic below does not work in the presence of transparent naming.
  // The current implementation renames files on UFS if they are marked as persisted. They are
  // marked as persisted when they are closed. Thus, if the Alluxio path of the file being
  // written to changes before it is closed, renaming the temporary underlying file to its final
  // destination fails.
  public void basicRenameTest7() throws Exception {
    // Rename /dirA to /dirB, /dirA/fileA should become /dirB/fileA even if it was not closed

    Path dirA = new Path("/dirA");
    Path dirB = new Path("/dirB");
    Path fileA = new Path("/dirA/fileA");
    Path finalDst = new Path("/dirB/fileA");

    sTFS.mkdirs(dirA);
    FSDataOutputStream o = sTFS.create(fileA);
    o.writeBytes("Test Bytes");
    // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
    // FSDataOutputStream.hflush will be the new one.
    //#ifdef HADOOP1
    //o.sync();
    //#else
    o.hflush();
    //#endif

    Assert.assertTrue(sTFS.rename(dirA, dirB));

    Assert.assertFalse(sTFS.exists(dirA));
    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirB));
    Assert.assertTrue(sTFS.exists(finalDst));

    o.close();

    Assert.assertFalse(sTFS.exists(dirA));
    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirB));
    Assert.assertTrue(sTFS.exists(finalDst));
    cleanup(sTFS);
  }

  @Test
  public void errorRenameTest1() throws Exception {
    // Rename /dirA to /dirA/dirB should fail
    Path dirA = new Path("/dirA");
    Path finalDst = new Path("/dirA/dirB");

    sTFS.mkdirs(dirA);

    Assert.assertFalse(sTFS.rename(dirA, finalDst));

    Assert.assertFalse(sTFS.exists(finalDst));
    Assert.assertTrue(sTFS.exists(dirA));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA", "dirB")));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirB")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(dirA));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirB")));
  }

  @Test
  public void errorRenameTest2() throws Exception {
    // Rename /fileA to /fileB should fail if /fileB exists
    Path fileA = new Path("/fileA");
    Path fileB = new Path("/fileB");

    create(sTFS, fileA);
    create(sTFS, fileB);

    Assert.assertFalse(sTFS.rename(fileA, fileB));

    Assert.assertTrue(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(fileB));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileB")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertFalse(sTFS.exists(fileB));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileB")));
  }

  @Test
  public void errorRenameTest3() throws Exception {
    // Rename /fileA to /dirA/fileA should fail if /dirA/fileA exists
    Path fileA = new Path("/fileA");
    Path dirA = new Path("/dirA");
    Path finalDst = new Path("/dirA/fileA");

    create(sTFS, fileA);
    create(sTFS, finalDst);

    Assert.assertFalse(sTFS.rename(fileA, dirA));

    Assert.assertTrue(sTFS.exists(fileA));
    Assert.assertTrue(sTFS.exists(dirA));
    Assert.assertTrue(sTFS.exists(finalDst));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertTrue(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirA", "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertFalse(sTFS.exists(dirA));
    Assert.assertFalse(sTFS.exists(finalDst));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
    Assert.assertFalse(sUfs.isDirectory(PathUtils.concatPath(sUfsRoot, "dirA")));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "dirA", "fileA")));
  }

  @Test
  public void errorRenameTest4() throws Exception {
    // Rename /fileA to an nonexistent path should fail
    Path fileA = new Path("/fileA");
    Path nonexistentPath = new Path("/doesNotExist/fileA");

    create(sTFS, fileA);

    Assert.assertFalse(sTFS.rename(fileA, nonexistentPath));

    Assert.assertTrue(sTFS.exists(fileA));
    Assert.assertTrue(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));

    cleanup(sTFS);

    Assert.assertFalse(sTFS.exists(fileA));
    Assert.assertFalse(sUfs.isFile(PathUtils.concatPath(sUfsRoot, "fileA")));
  }
}

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

package alluxio.client.fuse.dora.hdfs3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.LibFuse;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;

public class FuseHdfsIntegrationTest extends AbstractFuseHdfsIntegrationTest {
  @BeforeClass
  public static void beforeClass() {
    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
  }

  @Test
  public void createDeleteEmptyFile() throws Exception {
    String testFile = Paths.get(MOUNT_POINT, "/createDeleteEmptyFile").toString();
    File file = new File(testFile);
    assertFalse(file.exists());
    new FileOutputStream(testFile).close();
    assertTrue(file.exists());
    assertTrue(mHdfs.exists(new Path("/createDeleteEmptyFile")));
    assertEquals(0, file.length());
    assertTrue(file.isFile());
    assertTrue(file.delete());
    assertFalse(file.exists());
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
    // Verify on fuse
    try (FileInputStream inputStream = new FileInputStream(testFile)) {
      byte[] res = new byte[content.length];
      assertEquals(content.length, inputStream.read(res));
      assertEquals(Arrays.toString(content), Arrays.toString(res));
    }
    // Verify on Hdfs
    try (FSDataInputStream inputStream = mHdfs.open(new Path("/writeThenRead"))) {
      byte[] res = new byte[content.length];
      assertEquals(content.length, inputStream.read(res));
      assertEquals(Arrays.toString(content), Arrays.toString(res));
    }
    assertTrue(file.delete());
    assertFalse(file.exists());
  }
}

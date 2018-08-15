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

package alluxio.master.backcompat.ops;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.master.backcompat.FsTestOp;

import org.junit.Assert;

import java.io.File;

/**
 * Test for mounting and unmounting.
 */
public final class Mount extends FsTestOp {
  private static final AlluxioURI ALLUXIO_PATH = new AlluxioURI("/mount");

  private static final String LOCAL_FS_MOUNT_DIR = "/tmp/alluxioMount";
  // This creates a requirement that /tmp/alluxioMount exists and is readable by the test
  private static final AlluxioURI UFS_PATH = new AlluxioURI(LOCAL_FS_MOUNT_DIR);

  @Override
  public void apply(FileSystem fs) throws Exception {
    new File(LOCAL_FS_MOUNT_DIR).mkdirs();
    fs.mount(ALLUXIO_PATH, UFS_PATH);
    fs.unmount(ALLUXIO_PATH);
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    Assert.assertFalse("Mounted and unmounted directory should not exist", fs.exists(ALLUXIO_PATH));
  }
}

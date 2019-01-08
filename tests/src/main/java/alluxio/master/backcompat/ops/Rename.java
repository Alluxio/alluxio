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
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.master.backcompat.FsTestOp;
import alluxio.master.backcompat.Utils;

import org.junit.Assert;

/**
 * Test for renaming a file.
 */
public final class Rename extends FsTestOp {
  private static final AlluxioURI SRC = new AlluxioURI("/fileToRename");
  private static final AlluxioURI DST = new AlluxioURI("/fileRenameTarget");

  @Override
  public void apply(FileSystem fs) throws Exception {
    Utils.createFile(fs, SRC);
    fs.rename(SRC, DST);

    fs.createDirectory(new AlluxioURI("/renameDir/a/b"),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    fs.rename(new AlluxioURI("/renameDir/a"), new AlluxioURI("/renameDir/c"));
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    Assert.assertFalse(fs.exists(SRC));
    Assert.assertTrue(fs.exists(DST));

    Assert.assertFalse(fs.exists(new AlluxioURI("/renameDir/a")));
    Assert.assertTrue(fs.exists(new AlluxioURI("/renameDir/c")));
    Assert.assertTrue(fs.exists(new AlluxioURI("/renameDir/c/b")));
  }
}

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.grpc.DeletePOptions;
import alluxio.master.backcompat.FsTestOp;
import alluxio.master.backcompat.Utils;

import java.util.Arrays;

/**
 * Test for file deletion.
 */
public final class Delete extends FsTestOp {
  private static final AlluxioURI PATH = new AlluxioURI("/pathToDelete");
  private static final AlluxioURI NESTED = new AlluxioURI("/deleteFile/a");
  private static final AlluxioURI RECURSIVE = new AlluxioURI("/deleteRecursive/a/b");

  @Override
  public void apply(FileSystem fs) throws Exception {
    Utils.createFile(fs, PATH);
    fs.delete(PATH);

    Utils.createFile(fs, NESTED);
    fs.delete(NESTED);

    Utils.createFile(fs, RECURSIVE);
    fs.delete(RECURSIVE.getParent(), DeletePOptions.newBuilder().setRecursive(true).build());
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    for (AlluxioURI deleted : Arrays.asList(PATH, NESTED, RECURSIVE)) {
      assertFalse(fs.exists(deleted));
    }
    assertTrue(fs.exists(NESTED.getParent()));
    assertFalse(fs.exists(RECURSIVE.getParent()));
    assertTrue(fs.exists(RECURSIVE.getParent().getParent()));
  }
}

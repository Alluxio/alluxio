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
import alluxio.master.backcompat.Utils;

import org.junit.Assert;

/**
 * Test for file creation.
 */
public final class CreateFile extends FsTestOp {
  private static final AlluxioURI PATH = new AlluxioURI("/createFile");

  @Override
  public void apply(FileSystem fs) throws Exception {
    Utils.createFile(fs, PATH);
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    Assert.assertTrue("Created file should exist", fs.exists(PATH));
  }
}

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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.master.backcompat.TestOp;
import alluxio.master.backcompat.Utils;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Assert;

/**
 * Test for persisting a file.
 */
public final class PersistFile implements TestOp {
  private static final AlluxioURI PATH = new AlluxioURI("/fileToPersist");

  @Override
  public void apply(Clients clients) throws Exception {
    FileSystem fs = clients.getFs();
    Utils.createFile(fs, PATH);
    clients.getFileSystemMaster().scheduleAsyncPersist(PATH);
    CommonUtils.waitFor("file to be async persisted", () -> {
      try {
        return fs.getStatus(PATH).isPersisted();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }

  @Override
  public void check(Clients clients) throws Exception {
    Assert.assertTrue("Persisted file should be persisted",
        clients.getFs().getStatus(PATH).isPersisted());
  }
}

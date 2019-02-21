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

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.master.backcompat.TestOp;
import alluxio.master.backcompat.Utils;
import alluxio.multi.process.Clients;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.WaitForOptions;

import java.util.Arrays;

/**
 * Test for persisting a file.
 */
public final class PersistFile implements TestOp {
  private static final AlluxioURI PATH = new AlluxioURI("/fileToPersist");
  private static final AlluxioURI NESTED_PATH = new AlluxioURI("/persistFileDir/a");

  @Override
  public void apply(Clients clients) throws Exception {
    FileSystem fs = clients.getFileSystemClient();
    Utils.createFile(fs, PATH);
    clients.getFileSystemMasterClient().scheduleAsyncPersist(PATH,
        FileSystemOptions.scheduleAsyncPersistDefaults(ServerConfiguration.global()));
    Utils.createFile(fs, NESTED_PATH);
    clients.getFileSystemMasterClient().scheduleAsyncPersist(NESTED_PATH,
        FileSystemOptions.scheduleAsyncPersistDefaults(ServerConfiguration.global()));
    CommonUtils.waitFor("file to be async persisted", () -> {
      try {
        return fs.getStatus(PATH).isPersisted() && fs.getStatus(NESTED_PATH).isPersisted();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }

  @Override
  public void check(Clients clients) throws Exception {
    for (AlluxioURI uri : Arrays.asList(PATH, NESTED_PATH, NESTED_PATH.getParent())) {
      assertTrue(clients.getFileSystemClient().getStatus(uri).isPersisted());
    }
  }
}

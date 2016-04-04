/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Test the internal implementation of alluxio Master via a
 * {@link FileSystemMasterClient}.
 *
 * <p>
 */
public final class FileSystemMasterClientIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private Configuration mMasterConfiguration = null;

  @Before
  public final void before() throws Exception {
    mMasterConfiguration = mLocalAlluxioClusterResource.get().getMasterConf();
  }

  @Test
  public void openCloseTest() throws AlluxioException, IOException {
    FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), mMasterConfiguration);
    AlluxioURI file = new AlluxioURI("/file");
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    fsMasterClient.createFile(file, CreateFileOptions.defaults());
    Assert.assertNotNull(fsMasterClient.getStatus(file));
    fsMasterClient.disconnect();
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    Assert.assertNotNull(fsMasterClient.getStatus(file));
    fsMasterClient.close();
  }

  @Test(timeout = 3000, expected = AlluxioException.class)
  public void getFileInfoReturnsOnErrorTest() throws IOException, AlluxioException {
    // This test was created to show that an infinite loop occurs.
    // The timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), mMasterConfiguration);
    fsMasterClient.getStatus(new AlluxioURI("/doesNotExist"));
    fsMasterClient.close();
  }
}

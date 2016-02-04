/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;

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
    mMasterConfiguration = mLocalAlluxioClusterResource.get().getMasterTachyonConf();
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

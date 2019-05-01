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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests reinitializing {@link FileSystemContext}.
 */
public final class FileSystemContextReinitIntegrationTest extends BaseIntegrationTest {
  private FileSystemContext mContext;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws IOException {
    mContext = FileSystemContext.create(ServerConfiguration.global());
    mContext.getClientContext().updateConfigurationDefaults(mContext.getMasterAddress());
  }

  @Test
  public void noConfUpdate() throws Exception {
    String clusterConfHash = mContext.getClientContext().getClusterConfHash();
    Assert.assertNotNull(clusterConfHash);
    String pathConfHash = mContext.getClientContext().getPathConfHash();
    Assert.assertNotNull(pathConfHash);

    mContext.reinit();

    Assert.assertEquals(clusterConfHash, mContext.getClientContext().getClusterConfHash());
    Assert.assertEquals(pathConfHash, mContext.getClientContext().getPathConfHash());
  }

  @Test
  public void updateClusterConf() throws Exception {
    PropertyKey keyToUpdate = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
    String updatedValue = ReadType.NO_CACHE.toString();

    String clusterConfHash = mContext.getClientContext().getClusterConfHash();
    Assert.assertNotNull(clusterConfHash);
    Assert.assertNotEquals(updatedValue, ServerConfiguration.get(keyToUpdate));
    String pathConfHash = mContext.getClientContext().getPathConfHash();
    Assert.assertNotNull(pathConfHash);

    mLocalAlluxioClusterResource.get().stopMasters();
    ServerConfiguration.set(keyToUpdate, updatedValue);
    mLocalAlluxioClusterResource.get().startMasters();

    mContext.reinit();

    Assert.assertNotEquals(clusterConfHash, mContext.getClientContext().getClusterConfHash());
    Assert.assertEquals(updatedValue, ServerConfiguration.get(keyToUpdate));
    Assert.assertEquals(pathConfHash, mContext.getClientContext().getPathConfHash());
  }

  @Test
  public void updatePathConf() throws Exception {
    AlluxioURI pathToUpdate = new AlluxioURI("/path/to/update");
    PropertyKey keyToUpdate = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
    String updatedValue = ReadType.NO_CACHE.toString();

    String clusterConfHash = mContext.getClientContext().getClusterConfHash();
    Assert.assertNotNull(clusterConfHash);
    String pathConfHash = mContext.getClientContext().getPathConfHash();
    Assert.assertNotNull(pathConfHash);
    Assert.assertFalse(mContext.getClientContext().getPathConf().getConfiguration(
        pathToUpdate, keyToUpdate).isPresent());

    MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(mContext.getClientContext()).build());
    client.setPathConfiguration(pathToUpdate, keyToUpdate, updatedValue);

    mContext.reinit();

    Assert.assertEquals(clusterConfHash, mContext.getClientContext().getClusterConfHash());
    Assert.assertNotEquals(pathConfHash, mContext.getClientContext().getPathConfHash());
    Assert.assertEquals(updatedValue, mContext.getClientContext().getPathConf().getConfiguration(
        pathToUpdate, keyToUpdate).get().get(keyToUpdate));
  }
}

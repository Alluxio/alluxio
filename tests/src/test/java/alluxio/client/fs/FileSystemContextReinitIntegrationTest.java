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

import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests reinitializing {@link FileSystemContext}.
 */
public final class FileSystemContextReinitIntegrationTest extends BaseIntegrationTest {
  private FileSystemContext mContext;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws Exception {
    mContext = FileSystemContext.create(ServerConfiguration.global());
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
}

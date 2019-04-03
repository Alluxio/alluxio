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

package alluxio.client.cli.fsadmin.pathconf;

import alluxio.cli.fsadmin.FileSystemAdminShell;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.cli.fs.AbstractShellIntegrationTest;
import alluxio.client.file.FileSystemContext;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterClientContext;

import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;

/**
 * Base class for integrations tests of pathConf commands.
 *
 * It sets path level configurations for DIR1 and DIR2 before all tests.
 */
public class PathConfTest extends AbstractShellIntegrationTest {
  protected static final String DIR1 = "/path/to/dir1";
  protected static final PropertyKey PROPERTY_KEY1 = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  protected static final String PROPERTY_VALUE1 = ReadType.NO_CACHE.toString();
  protected static final String DIR2 = "/path/to/dir2/";
  protected static final PropertyKey PROPERTY_KEY2 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  protected static final String PROPERTY_VALUE2 = WriteType.THROUGH.toString();

  protected MetaMasterConfigClient mMetaConfig;
  protected FileSystemAdminShell mFsAdminShell;

  private void setPathConfigurations(MetaMasterConfigClient client) throws Exception {
    client.setPathConfiguration(DIR1, PROPERTY_KEY1, PROPERTY_VALUE1);
    client.setPathConfiguration(DIR2, PROPERTY_KEY2, PROPERTY_VALUE2);
  }

  @Before
  public void before() throws Exception {
    FileSystemContext metaCtx = FileSystemContext.create(ServerConfiguration.global());
    mMetaConfig = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(metaCtx.getClientContext()).build());
    setPathConfigurations(mMetaConfig);
    InetSocketAddress address = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getAddress();
    FileSystemContext fsCtx = FileSystemContext.create(ServerConfiguration.global());
    fsCtx.getClientContext().updateConfigurationDefaults(address);
    mFsAdminShell = new FileSystemAdminShell((InstancedConfiguration) fsCtx.getConf());
  }

  @After
  public final void after() throws Exception {
    mFsAdminShell.close();
  }
}

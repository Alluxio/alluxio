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

import alluxio.AlluxioURI;
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

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Tests for pathConf list command.
 */
public class ListCommandIntegrationTest extends AbstractShellIntegrationTest {
  private static final String DIR1 = "/path/to/dir1";
  private static final PropertyKey PROPERTY_KEY1 = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE1 = ReadType.NO_CACHE.toString();
  private static final String DIR2 = "/path/to/dir2";
  private static final PropertyKey PROPERTY_KEY2 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE2 = WriteType.THROUGH.toString();

  /**
   * Sets path level configurations through meta master client, and update client configurations
   * from meta master afterwards.
   *
   * @return the configuration after updating from meta master
   */
  private InstancedConfiguration setPathConfigurations() throws Exception {
    FileSystemContext metaCtx = FileSystemContext.create(ServerConfiguration.global());
    MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(metaCtx.getClientContext()).build());
    client.setPathConfiguration(new AlluxioURI(DIR1), PROPERTY_KEY1, PROPERTY_VALUE1);
    client.setPathConfiguration(new AlluxioURI(DIR2), PROPERTY_KEY2, PROPERTY_VALUE2);
    InetSocketAddress address = sLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getAddress();
    FileSystemContext fsCtx = FileSystemContext.create(ServerConfiguration.global());
    fsCtx.getClientContext().loadConf(address, true, true);
    return (InstancedConfiguration) fsCtx.getClusterConf();
  }

  @Test
  public void listEmpty() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      int ret = shell.run("pathConf", "list");
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals("", output);
    }
  }

  @Test
  public void list() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "list");
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals(DIR1 + "\n" + DIR2 + "\n", output);
    }
  }
}

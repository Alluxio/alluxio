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
 * Tests for pathConf show command.
 */
public class ShowCommandIntegrationTest extends AbstractShellIntegrationTest {
  private static final String DIR0 = "/a";
  private static final String DIR1 = "/a/b";
  private static final PropertyKey PROPERTY_KEY11 = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE11 = ReadType.NO_CACHE.toString();
  private static final PropertyKey PROPERTY_KEY12 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE12 = WriteType.MUST_CACHE.toString();
  private static final String DIR2 = "/a/b/c";
  private static final PropertyKey PROPERTY_KEY2 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE2 = WriteType.THROUGH.toString();
  private static final String DIR3 = "/a/b/c/d";

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
    client.setPathConfiguration(new AlluxioURI(DIR1), PROPERTY_KEY11, PROPERTY_VALUE11);
    client.setPathConfiguration(new AlluxioURI(DIR1), PROPERTY_KEY12, PROPERTY_VALUE12);
    client.setPathConfiguration(new AlluxioURI(DIR2), PROPERTY_KEY2, PROPERTY_VALUE2);
    InetSocketAddress address = sLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
        .getAddress();
    FileSystemContext fsCtx = FileSystemContext.create(ServerConfiguration.global());
    fsCtx.getClientContext().loadConf(address, true, true);
    return (InstancedConfiguration) fsCtx.getClusterConf();
  }

  private String format(PropertyKey key, String value) {
    return key.getName() + "=" + value;
  }

  @Test
  public void showDir0() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", DIR0);
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals("", output);
    }
  }

  @Test
  public void showDir1() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", DIR1);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY11, PROPERTY_VALUE11) + "\n"
          + format(PROPERTY_KEY12, PROPERTY_VALUE12) + "\n";
      String output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void showDir2() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", DIR2);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY2, PROPERTY_VALUE2) + "\n";
      String output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void showDir3() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", DIR3);
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals("", output);
    }
  }

  @Test
  public void showResolveDir0() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", "--all", DIR0);
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals("", output);
    }
  }

  @Test
  public void showResolveDir1() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", "--all", DIR1);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY11, PROPERTY_VALUE11) + "\n"
          + format(PROPERTY_KEY12, PROPERTY_VALUE12) + "\n";
      String output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void showResolveDir2() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", "--all", DIR2);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY11, PROPERTY_VALUE11) + "\n"
          + format(PROPERTY_KEY2, PROPERTY_VALUE2) + "\n";
      String output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void showResolveDir3() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(setPathConfigurations())) {
      int ret = shell.run("pathConf", "show", "--all", DIR3);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY11, PROPERTY_VALUE11) + "\n"
          + format(PROPERTY_KEY2, PROPERTY_VALUE2) + "\n";
      String output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }
}

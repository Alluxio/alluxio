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

package alluxio.client.cli.fsadmin;

import alluxio.cli.fsadmin.FileSystemAdminShell;
import alluxio.client.cli.fs.AbstractShellIntegrationTest;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;

import org.junit.After;
import org.junit.Before;

public class AbstractFsAdminShellTest extends AbstractShellIntegrationTest {
  protected LocalAlluxioCluster mLocalAlluxioCluster = null;
  protected FileSystemAdminShell mFsAdminShell = null;

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = sLocalAlluxioClusterResource.get();
    mFsAdminShell = new FileSystemAdminShell(ServerConfiguration.global());
  }

  @After
  public final void after() throws Exception {
    mFsAdminShell.close();
  }
}

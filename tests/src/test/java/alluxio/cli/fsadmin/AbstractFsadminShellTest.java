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

package alluxio.cli.fsadmin;

import alluxio.BaseIntegrationTest;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.LocalAlluxioCluster;
import alluxio.PropertyKey;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;

public class AbstractFsadminShellTest extends BaseIntegrationTest{
  private static final int SIZE_BYTES = Constants.MB * 16;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE).build();
  protected LocalAlluxioCluster mLocalAlluxioCluster = null;
  protected FileSystemAdminShell mFsAdminShell = null;
  protected ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  protected ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();

  @Rule
  public ExpectedException mException = ExpectedException.none();
  @Rule
  public SystemOutRule mOutRule = new SystemOutRule(mOutput);
  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFsAdminShell = new FileSystemAdminShell();
  }

  @After
  public final void after() throws Exception {
    mFsAdminShell.close();
  }
}

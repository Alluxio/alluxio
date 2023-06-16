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

package alluxio.client.cli.fs;

import alluxio.Constants;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * The base class for all the shell integration test.
 */
public abstract class AbstractDoraShellIntegrationTest extends BaseIntegrationTest {
  protected static final int SIZE_BYTES = Constants.MB * 16;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  public AbstractDoraShellIntegrationTest(int numWorkers) throws IOException {
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
        .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
        .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
        .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Long.MAX_VALUE)
        .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
        .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, SIZE_BYTES / 2)
        .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
        .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
        .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
        .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
        .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "1GB")
        .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
        .setNumWorkers(numWorkers)
        .setStartCluster(false)
        .build();
  }

  @Before
  public void before() throws Exception {
    mLocalAlluxioClusterResource
        .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, mTestFolder.getRoot().getAbsolutePath())
        .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
            mTestFolder.getRoot().getAbsolutePath());
  }

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;

  public ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  public ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public SystemOutRule mOutRule = new SystemOutRule(mOutput);

  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);
}

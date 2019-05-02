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
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests reinitializing {@link FileSystemContext}.
 */
public final class FileSystemContextReinitIntegrationTest extends BaseIntegrationTest {
  private static final AlluxioURI PATH_TO_UPDATE = new AlluxioURI("/path/to/update");
  private static final PropertyKey KEY_TO_UPDATE = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  private static final String UPDATED_VALUE = ReadType.NO_CACHE.toString();

  private FileSystemContext mContext;
  private String mClusterConfHash;
  private String mPathConfHash;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.META_MASTER_CONFIG_HASH_SYNC);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws IOException {
    mContext = FileSystemContext.create(ServerConfiguration.global());
    mContext.getClientContext().updateConfigurationDefaults(mContext.getMasterAddress());
    mClusterConfHash = mContext.getClientContext().getClusterConfHash();
    Assert.assertNotNull(mClusterConfHash);
    mPathConfHash = mContext.getClientContext().getPathConfHash();
    Assert.assertNotNull(mPathConfHash);
  }

  @Test
  public void noConfUpdateAndNoRestart() throws Exception {
    mContext.reinit();
    checkHash(false, false);
  }

  @Test
  public void restartWithoutConfUpdate() throws Exception {
    restartMasters();
    mContext.reinit();
    checkHash(false, false);
  }

  @Test
  public void clusterConfUpdate() throws Exception {
    checkClusterConfBeforeUpdate();
    updateClusterConf();
    mContext.reinit();
    checkClusterConfAfterUpdate();
    checkHash(true, false);
  }

  @Test
  public void pathConfUpdate() throws Exception {
    checkPathConfBeforeUpdate();
    updatePathConf();
    mContext.reinit();
    checkPathConfAfterUpdate();
    checkHash(false, true);
  }

  @Test(timeout = 1000 * 30)
  public void configHashSync() throws Exception {
    checkClusterConfBeforeUpdate();
    checkPathConfBeforeUpdate();
    updateClusterConf();
    updatePathConf();
    HeartbeatScheduler.execute(HeartbeatThread.generateThreadName(
        HeartbeatContext.META_MASTER_CONFIG_HASH_SYNC, mContext.getAppId()));
    checkClusterConfAfterUpdate();
    checkPathConfAfterUpdate();
    checkHash(true, true);
  }

  private void restartMasters() throws Exception {
    mLocalAlluxioClusterResource.get().stopMasters();
    mLocalAlluxioClusterResource.get().startMasters();
  }

  private void updateClusterConf() throws Exception {
    mLocalAlluxioClusterResource.get().stopMasters();
    ServerConfiguration.set(KEY_TO_UPDATE, UPDATED_VALUE);
    mLocalAlluxioClusterResource.get().startMasters();
  }

  private void updatePathConf() throws Exception {
    MetaMasterConfigClient client = new RetryHandlingMetaMasterConfigClient(
        MasterClientContext.newBuilder(mContext.getClientContext()).build());
    client.setPathConfiguration(PATH_TO_UPDATE, KEY_TO_UPDATE, UPDATED_VALUE);
  }

  private void checkClusterConfBeforeUpdate() {
    Assert.assertNotEquals(UPDATED_VALUE, mContext.getClientContext().getConf().get(KEY_TO_UPDATE));
  }

  private void checkClusterConfAfterUpdate() {
    Assert.assertEquals(UPDATED_VALUE, mContext.getClientContext().getConf().get(KEY_TO_UPDATE));
  }

  private void checkPathConfBeforeUpdate() {
    Assert.assertFalse(mContext.getClientContext().getPathConf().getConfiguration(
        PATH_TO_UPDATE, KEY_TO_UPDATE).isPresent());
  }

  private void checkPathConfAfterUpdate() {
    Assert.assertEquals(UPDATED_VALUE, mContext.getClientContext().getPathConf().getConfiguration(
        PATH_TO_UPDATE, KEY_TO_UPDATE).get().get(KEY_TO_UPDATE));
  }

  private void checkHash(boolean clusterConfHashUpdated, boolean pathConfHashUpdated) {
    Assert.assertEquals(clusterConfHashUpdated, !mContext.getClientContext().getClusterConfHash()
        .equals(mClusterConfHash));
    Assert.assertEquals(pathConfHashUpdated, !mContext.getClientContext().getPathConfHash()
        .equals(mPathConfHash));
  }
}

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

package alluxio.zookeeper;

import static org.junit.Assert.assertNull;

import alluxio.AlluxioOperationThread;
import alluxio.BaseIntegrationTest;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Alluxio high availability when Zookeeper has failures.
 */
public class ZookeeperFailureIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperFailureIntegrationTest.class);

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1000"));

  @Rule
  public MultiProcessCluster mCluster = MultiProcessCluster.newBuilder()
      .setClusterName("ZookeeperFailure")
      .addProperty(PropertyKey.ZOOKEEPER_ENABLED, "true")
      .setNumMasters(2)
      .setNumWorkers(1)
      .build();

  @Test
  public void zkFailure() throws Exception {
    final AlluxioOperationThread thread = new AlluxioOperationThread(mCluster.getFileSystemClient());
    thread.start();
    CommonUtils.waitFor("a successful operation to be performed", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return thread.successes() > 0;
      }
    });
    assertNull(thread.getLatestFailure());
    mCluster.stopZk();
    long zkStopTime = System.currentTimeMillis();
    CommonUtils.waitFor("operations to start failing", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return thread.getLatestFailure() != null;
      }
    });
    LOG.info("First operation failed {}ms after stopping the Zookeeper cluster",
        System.currentTimeMillis() - zkStopTime);
    final long successes = thread.successes();
    mCluster.startZk();
    long zkStartTime = System.currentTimeMillis();
    CommonUtils.waitFor("another successful operation to be performed", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return thread.successes() > successes;
      }
    });
    LOG.info("Recovered after {}ms", System.currentTimeMillis() - zkStartTime);
    mCluster.notifySuccess();
  }
}

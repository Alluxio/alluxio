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

package alluxio.client.hadoop;

import alluxio.Constants;
import alluxio.hadoop.FileSystem;
import alluxio.master.ZkMasterInquireClient.ZkMasterConnectDetails;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Test;

import java.net.URI;

/**
 * Integration tests for using URIs with connect details in authorities to connect to
 * Alluxio clusters through {@link FileSystem}.
 */
public class FileSystemUriIntegrationTest extends BaseIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
  public MultiProcessCluster mCluster;

  @After
  public void after() throws Exception {
    mCluster.destroy();
  }

  @Test
  public void zookeeperUriTest() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.MULTI_PROCESS_ZOOKEEPER)
        .setClusterName("ZookeeperUriFileSystemIntegrationTest")
        .setNumMasters(3)
        .setNumWorkers(2)
        .setDeployMode(DeployMode.ZOOKEEPER_HA)
        .build();
    mCluster.start();
    // Get the zookeeper address
    ZkMasterConnectDetails connectDetails =
        (ZkMasterConnectDetails) mCluster.getMasterInquireClient().getConnectDetails();
    String zkAddress = connectDetails.getZkAddress();

    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create("alluxio://zk@" + zkAddress + "/tmp/path.txt");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf);

    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

    Path file = new Path("/testFile-Zookeeper");
    FsPermission permission = FsPermission.createImmutable((short) 0666);
    FSDataOutputStream o = fs.create(file, permission, false /* ignored */, 10 /* ignored */,
        (short) 1 /* ignored */, 512 /* ignored */, null /* ignored */);
    o.writeBytes("Test Bytes");
    o.close();
    // with mark of delete-on-exit, the close method will try to delete it
    fs.deleteOnExit(file);
    fs.close();
    mCluster.notifySuccess();
  }
}

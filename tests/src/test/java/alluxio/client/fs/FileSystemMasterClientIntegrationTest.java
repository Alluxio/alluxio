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
import alluxio.ClientContext;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.FileSystemOptions;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests the internal implementation of alluxio Master via a {@link FileSystemMasterClient}.
 */
public final class FileSystemMasterClientIntegrationTest extends BaseIntegrationTest {
  private static final GetStatusPOptions GET_STATUS_OPTIONS =
      FileSystemOptions.getStatusDefaults(ServerConfiguration.global());

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Test
  public void openClose() throws AlluxioException, IOException {
    FileSystemMasterClient fsMasterClient =
        FileSystemMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    AlluxioURI file = new AlluxioURI("/file");
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    fsMasterClient.createFile(file,
        FileSystemOptions.createFileDefaults(ServerConfiguration.global()));
    Assert.assertNotNull(fsMasterClient.getStatus(file, GET_STATUS_OPTIONS));
    fsMasterClient.disconnect();
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    Assert.assertNotNull(fsMasterClient.getStatus(file, GET_STATUS_OPTIONS));
    fsMasterClient.close();
  }

  @Test(timeout = 3000, expected = NotFoundException.class)
  public void getFileInfoReturnsOnError() throws Exception {
    // This test was created to show that an infinite loop occurs.
    // The timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    FileSystemMasterClient fsMasterClient =
        FileSystemMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    fsMasterClient.getStatus(new AlluxioURI("/doesNotExist"), GET_STATUS_OPTIONS);
    fsMasterClient.close();
  }

  @Test(timeout = 300000)
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_RPC_RETRY_MAX_DURATION, "10s"})
  public void masterUnavailable() throws Exception {
    mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().stop();

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(3000);
          mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().start();
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    });
    thread.start();
    FileSystem fileSystem = mLocalAlluxioClusterResource.get().getClient();
    fileSystem.listStatus(new AlluxioURI("/"));
    thread.join();
  }
}

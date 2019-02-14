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

package alluxio.master.backcompat.ops;

import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.grpc.UfsPMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.master.backcompat.TestOp;
import alluxio.multi.process.Clients;

/**
 * Test for updating UFS modes.
 */
public final class UpdateUfsMode implements TestOp {

  @Override
  public void apply(Clients clients) throws Exception {
    FileSystemMasterClient masterClient = clients.getFileSystemMasterClient();
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModePOptions.newBuilder().setUfsMode(UfsPMode.READ_ONLY).build());
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModePOptions.newBuilder().setUfsMode(UfsPMode.NO_ACCESS).build());
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModePOptions.newBuilder().setUfsMode(UfsPMode.READ_WRITE).build());
  }

  @Override
  public void check(Clients clients) throws Exception {
    FileSystemMasterClient masterClient = clients.getFileSystemMasterClient();
    assertFalse(masterClient.getMountTable().get("/").getReadOnly());
  }
}

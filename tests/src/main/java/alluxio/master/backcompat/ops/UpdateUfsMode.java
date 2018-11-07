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
import alluxio.client.file.options.UpdateUfsModeOptions;
import alluxio.master.backcompat.TestOp;
import alluxio.multi.process.Clients;
import alluxio.underfs.UnderFileSystem.UfsMode;

/**
 * Test for updating UFS modes.
 */
public final class UpdateUfsMode implements TestOp {

  @Override
  public void apply(Clients clients) throws Exception {
    FileSystemMasterClient masterClient = clients.getFileSystemMasterClient();
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModeOptions.defaults().setUfsMode(UfsMode.READ_ONLY));
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModeOptions.defaults().setUfsMode(UfsMode.NO_ACCESS));
    masterClient.updateUfsMode(new AlluxioURI("/"),
        UpdateUfsModeOptions.defaults().setUfsMode(UfsMode.READ_WRITE));
  }

  @Override
  public void check(Clients clients) throws Exception {
    FileSystemMasterClient masterClient = clients.getFileSystemMasterClient();
    assertFalse(masterClient.getMountTable().get("/").getReadOnly());
  }
}

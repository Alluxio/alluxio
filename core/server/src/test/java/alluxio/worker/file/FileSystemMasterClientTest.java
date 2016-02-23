/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.WorkerContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

/**
 * Tests for the {@link FileSystemMasterClient} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystemMasterClient.class)
public class FileSystemMasterClientTest {

  /**
   * Tests for an unsupported version.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void unsupportedVersionTest() throws Exception {
    // Client context needs to be initialized before the file system context can be used.
    WorkerContext.reset();

    FileSystemMasterWorkerService.Client mock =
        PowerMockito.mock(FileSystemMasterWorkerService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);

    FileSystemMasterClient client =
        new FileSystemMasterClient(NetworkAddressUtils.getConnectAddress(
            NetworkAddressUtils.ServiceType.MASTER_RPC, WorkerContext.getConf()),
            WorkerContext.getConf());

    try {
      Whitebox.invokeMethod(client, "checkVersion", mock,
          Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
          Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION, 0), e.getMessage());
    }
  }
}

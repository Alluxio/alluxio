/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.file;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.exception.ExceptionMessage;
import tachyon.thrift.FileSystemMasterWorkerService;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerContext;

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

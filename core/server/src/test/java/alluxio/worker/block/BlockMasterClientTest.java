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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.thrift.BlockMasterWorkerService;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

/**
 * Unit tests for {@link BlockMasterClient}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMasterClient.class)
public class BlockMasterClientTest {

  /**
   * Tests for an unsupported version.
   */
  @Test
  public void unsupportedVersionTest() throws Exception {
    BlockMasterWorkerService.Client mock = PowerMockito.mock(BlockMasterWorkerService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);

    BlockMasterClient client = new BlockMasterClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.MASTER_RPC));

    try {
      Whitebox.invokeMethod(client, "checkVersion", mock,
          Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.BLOCK_MASTER_WORKER_SERVICE_NAME,
          Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION, 0), e.getMessage());
    }
  }
}

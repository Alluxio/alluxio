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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.resource.CloseableResource;
import alluxio.thrift.BlockMasterClientService;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

/**
 * Tests for the {@link BlockMasterClient} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMasterClient.class)
public class BlockMasterClientTest {

  /**
   * Tests for an unsupported version.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void unsupportedVersionTest() throws Exception {
    BlockMasterClientService.Client mock = PowerMockito.mock(BlockMasterClientService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);

    try (CloseableResource<BlockMasterClient> clientResource =
        BlockStoreContext.INSTANCE.acquireMasterClientResource()) {
      Whitebox.invokeMethod(clientResource.get(), "checkVersion", mock,
          Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
          Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION, 0), e.getMessage());
    }
  }
}

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

package tachyon.client.block;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.exception.ExceptionMessage;
import tachyon.thrift.BlockMasterClientService;

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
    // Client context needs to be initialized before the block store context can be used.
    ClientContext.reset();

    BlockMasterClientService.Client mock = PowerMockito.mock(BlockMasterClientService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);

    BlockMasterClient client = BlockStoreContext.INSTANCE.acquireMasterClient();
    try {
      Whitebox.invokeMethod(client, "checkVersion", mock,
          Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
          Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION, 0), e.getMessage());
    } finally {
      BlockStoreContext.INSTANCE.releaseMasterClient(client);
    }
  }
}

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

package alluxio.client.block;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;

import javax.security.auth.Subject;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMasterClient.Factory.class)
public class BlockMasterClientPoolTest {
  @Test
  public void create() throws Exception {
    BlockMasterClient expectedClient = Mockito.mock(BlockMasterClient.class);
    PowerMockito.mockStatic(BlockMasterClient.Factory.class);
    Mockito.when(BlockMasterClient.Factory
        .create(Mockito.any(Subject.class), Mockito.any(InetSocketAddress.class)))
        .thenReturn(expectedClient);
    BlockMasterClient client;
    try (BlockMasterClientPool pool = new BlockMasterClientPool(null, null)) {
      client = pool.acquire();
      Assert.assertEquals(expectedClient, client);
      pool.release(client);
    }
    Mockito.verify(client).close();
  }
}

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMasterClient.Factory.class)
public class BlockMasterClientPoolTest {

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Test
  public void create() throws Exception {
    BlockMasterClient expectedClient = mock(BlockMasterClient.class);
    PowerMockito.mockStatic(BlockMasterClient.Factory.class);
    when(BlockMasterClient.Factory
        .create(any(MasterClientContext.class)))
        .thenReturn(expectedClient);
    BlockMasterClient client;
    ClientContext clientContext = ClientContext.create(mConf);
    MasterInquireClient masterInquireClient = MasterInquireClient.Factory
        .create(mConf, clientContext.getUserState());
    MasterClientContext masterClientContext = MasterClientContext.newBuilder(clientContext)
        .setMasterInquireClient(masterInquireClient).build();
    try (BlockMasterClientPool pool = new BlockMasterClientPool(masterClientContext)) {
      client = pool.acquire();
      assertEquals(expectedClient, client);
      pool.release(client);
    }
    verify(client).close();
  }
}

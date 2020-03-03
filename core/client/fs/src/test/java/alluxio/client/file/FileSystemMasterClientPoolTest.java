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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystemMasterClient.Factory.class)
public class FileSystemMasterClientPoolTest {
  @Test
  public void create() throws Exception {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    FileSystemMasterClient expectedClient = Mockito.mock(FileSystemMasterClient.class);
    PowerMockito.mockStatic(FileSystemMasterClient.Factory.class);
    Mockito.when(FileSystemMasterClient.Factory
        .create(Mockito.any(MasterClientContext.class)))
        .thenReturn(expectedClient);
    FileSystemMasterClient client;
    ClientContext clientContext = ClientContext.create(conf);
    MasterInquireClient masterInquireClient = MasterInquireClient.Factory
        .create(conf, clientContext.getUserState());
    MasterClientContext masterClientContext = MasterClientContext.newBuilder(clientContext)
        .setMasterInquireClient(masterInquireClient).build();
    try (FileSystemMasterClientPool pool = new FileSystemMasterClientPool(masterClientContext)) {
      client = pool.acquire();
      assertEquals(expectedClient, client);
      pool.release(client);
    }
    Mockito.verify(client).close();
  }
}

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

package tachyon.worker.lineage;

import java.io.IOException;

import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.exception.ExceptionMessage;
import tachyon.thrift.LineageMasterWorkerService;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LineageMasterClient.class)
public class LineageMasterClientTest {

  @Test
  public void unsupportedVersionTest() throws Exception {
    // Client context needs to be initialized before the lineage context can be used.
    WorkerContext.reset();

    LineageMasterWorkerService.Client mock =
        PowerMockito.mock(LineageMasterWorkerService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);
    PowerMockito.whenNew(LineageMasterWorkerService.Client.class).withAnyArguments()
        .thenReturn(mock);

    LineageMasterClient client =
        new LineageMasterClient(NetworkAddressUtils.getConnectAddress(
            NetworkAddressUtils.ServiceType.MASTER_RPC, WorkerContext.getConf()),
            WorkerContext.getConf());
    TMultiplexedProtocol mockProtocol = PowerMockito.mock(TMultiplexedProtocol.class);
    Whitebox.setInternalState(client, "mProtocol", mockProtocol);

    try {
      client.afterConnect();
      Assert.fail("connect() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.LINEAGE_MASTER_WORKER_SERVICE_NAME,
          Constants.LINEAGE_MASTER_WORKER_SERVICE_VERSION, 0), e.getMessage());
    }
  }
}

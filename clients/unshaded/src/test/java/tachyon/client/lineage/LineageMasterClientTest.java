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

package tachyon.client.lineage;

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
import tachyon.thrift.LineageMasterClientService;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LineageMasterClient.class)
public class LineageMasterClientTest {

  @Test
  public void unsupportedVersionTest() throws Exception {
    // Client context needs to be initialized before the lineage context can be used.
    ClientContext.reset();

    LineageMasterClientService.Client mock =
        PowerMockito.mock(LineageMasterClientService.Client.class);
    PowerMockito.when(mock.getServiceVersion()).thenReturn(0L);

    LineageMasterClient client = LineageContext.INSTANCE.acquireMasterClient();
    try {
      Whitebox.invokeMethod(client, "checkVersion", mock,
          Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME,
          Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION, 0), e.getMessage());
    } finally {
      LineageContext.INSTANCE.releaseMasterClient(client);
    }
  }
}

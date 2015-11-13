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

package tachyon.client.file;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.exception.ExceptionMessage;
import tachyon.thrift.FileSystemMasterService;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystemMasterClient.class)
public class FileSystemMasterClientTest {

  @Test
  public void unsupportedVersionTest() throws Exception {
    // Client context needs to be initialized before the file system context can be used.
    ClientContext.reset();

    FileSystemMasterClient client = FileSystemContext.INSTANCE.acquireMasterClient();
    FileSystemMasterService.Client mockClient =
        PowerMockito.mock(FileSystemMasterService.Client.class);
    PowerMockito.whenNew(FileSystemMasterService.Client.class).withAnyArguments()
        .thenReturn(mockClient);
    PowerMockito.when(mockClient.getServiceVersion()).thenReturn(0L);

    try {
      client.connect();
      Assert.fail("connect() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(
          Constants.FILE_SYSTEM_MASTER_SERVICE_VERSION, 0), e.getMessage());
    }
    FileSystemContext.INSTANCE.releaseMasterClient(client);
  }
}

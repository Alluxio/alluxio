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

package alluxio.cli;

import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import alluxio.util.JvmHeapDumper;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JvmHeapDumper.class, ClientProfiler.class})
@PowerMockIgnore("javax.management.*")
public class ClientProfilerTest {

  @Test
  public void abstractFsTest() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("testUser",
        new String[]{}));
    JvmHeapDumper heapDumpMock = PowerMockito.mock(JvmHeapDumper.class);
    whenNew(JvmHeapDumper.class).withAnyArguments().thenReturn(heapDumpMock);
    try {
      ClientProfiler.main(new String[]{"-c", "abstractfs", "--dry"});
    } catch (Exception e) {
      fail();
    }
  }
}

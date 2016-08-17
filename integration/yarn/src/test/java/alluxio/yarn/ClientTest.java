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

package alluxio.yarn;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;


/**
 * Unit tests for {@link Client}.
 * TODO: ALLUXIO-1503: add more unit test for alluxio.yarn.Client
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Client.class, YarnClient.class})
public final class ClientTest {

  private YarnClient mYarnClient;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    // Mock Yarn client
    PowerMockito.mockStatic(YarnClient.class);
    mYarnClient = (YarnClient) Mockito.mock(YarnClient.class);
    Mockito.when(YarnClient.createYarnClient()).thenReturn(mYarnClient);
  }

  /**
   * Tests that the main method properly reads args.
   */
  @Test(timeout = 10000)
  public void mainTest() throws Exception {
    Client mockClient = spy(new Client());
    Mockito.doNothing().when(mockClient).run();
    PowerMockito.whenNew(Client.class)
        .withNoArguments()
        .thenReturn(mockClient);

    Client.main(new String[] {
        "-resource_path", "hdfs://test",
        "-am_memory", "1024",
        "-am_vcores", "2"});
    PowerMockito.verifyNew(Client.class).withNoArguments();

    Mockito.verify(mockClient).run();
  }

  @Test(timeout = 10000)
  public void notEnoughResources() throws Exception {
    int masterMemInMB = (int) (Configuration.getBytes(
        PropertyKey.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB);
    YarnClientApplication mockYarnClientApplication = mock(YarnClientApplication.class);
    doReturn(mockYarnClientApplication).when(mYarnClient).createApplication();
    GetNewApplicationResponse mockNewApplicationResponse = mock(GetNewApplicationResponse.class);
    doReturn(mockNewApplicationResponse).when(mockYarnClientApplication)
        .getNewApplicationResponse();
    int maxAllocationMem = masterMemInMB/2;
    Resource resource = Resource.newInstance(maxAllocationMem, 1);
    doReturn(resource).when(mockNewApplicationResponse).getMaximumResourceCapability();
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.YARN_NOT_ENOUGH_HOSTS.getMessage("Alluxio Master",
        "memory", masterMemInMB, maxAllocationMem));
    Client client = new Client();
    client.run();
  }

}

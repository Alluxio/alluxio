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

package alluxio.worker.block;

import alluxio.AbstractClient;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.BlockMasterClientService;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit tests for {@link AbstractClient}.
 */
public final class AbstractClientTest {
  private static final String SERVICE_NAME = "Test Service Name";
  private static final int CLIENT_SERVICE_VERSION = 1;

  private final class TestClient extends AbstractClient {
    private TestClient() {
      super(Mockito.mock(InetSocketAddress.class), "");
    }

    @Override
    protected Client getClient() {
      return null;
    }

    @Override
    protected String getServiceName() {
      return SERVICE_NAME;
    }

    @Override
    protected long getServiceVersion() {
      return CLIENT_SERVICE_VERSION;
    }

    public void checkVersion(AlluxioService.Client thriftClient) throws IOException {
      checkVersion(thriftClient, Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION);
    }
  }

  /**
   * Tests for an unsupported version.
   */
  @Test
  public void unsupportedVersionTest() throws Exception {
    final BlockMasterClientService.Client thriftClient =
        Mockito.mock(BlockMasterClientService.Client.class);
    Mockito.when(thriftClient.getServiceVersion()).thenReturn(0L);

    try (TestClient client = new TestClient()) {
      client.checkVersion(thriftClient);
      Assert.fail("checkVersion() should fail");
    } catch (IOException e) {
      Assert.assertEquals(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(SERVICE_NAME,
          CLIENT_SERVICE_VERSION, 0), e.getMessage());
    }
  }
}

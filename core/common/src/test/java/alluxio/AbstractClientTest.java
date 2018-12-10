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

package alluxio;

import static alluxio.exception.ExceptionMessage.INCOMPATIBLE_VERSION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AlluxioServiceType;
import alluxio.retry.CountingRetry;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;

/**
 * Unit tests for {@link AbstractClient}.
 */
public final class AbstractClientTest {

  private static final String SERVICE_NAME = "Test Service Name";
  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private static class BaseTestClient extends AbstractClient {
    protected BaseTestClient() {
      super(null, null, () -> new CountingRetry(1));
    }

    @Override
    protected AlluxioServiceType getRemoteServiceType() {
      return AlluxioServiceType.UNKNOWN_SERVICE;
    }

    @Override
    protected String getServiceName() {
      return SERVICE_NAME;
    }

    @Override
    protected long getServiceVersion() {
      return 1;
    }
  }

  @Test
  public void connectFailToDetermineMasterAddress() throws Exception {
    alluxio.Client client = new BaseTestClient() {
      @Override
      public synchronized InetSocketAddress getAddress() throws UnavailableException {
        throw new UnavailableException("Failed to determine master address");
      }
    };
    mExpectedException.expect(UnavailableException.class);
    mExpectedException.expectMessage("Failed to determine address for Test Service Name");
    client.connect();
  }

  // TODO(ggezer) Fix after gRPC initialization
  // @Test
  public void unsupportedVersion() throws Exception {
    //mExpectedException.expect(IOException.class);
    mExpectedException.expectMessage(INCOMPATIBLE_VERSION.getMessage(SERVICE_NAME, 0, 1));
    final AbstractClient client = mock(BaseTestClient.class);
    when(client.getRemoteServiceVersion()).thenReturn(1L);
    client.checkVersion(0);
    client.close();
  }

  // @Test
  public void supportedVersion() throws Exception {
    final AbstractClient client = mock(BaseTestClient.class);
    when(client.getRemoteServiceVersion()).thenReturn(1L);
    client.checkVersion(1);
    client.close();
  }
}

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

import alluxio.conf.InstancedConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ServiceType;
import alluxio.retry.CountingRetry;
import alluxio.util.ConfigurationUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit tests for {@link AbstractClient}.
 */
public final class AbstractClientTest {

  private static final String SERVICE_NAME = "Test Service Name";
  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private static class BaseTestClient extends AbstractClient {
    private long mRemoteServiceVersion;

    protected BaseTestClient() {
      super(ClientContext.create(new InstancedConfiguration(ConfigurationUtils.defaults())), null,
          () -> new CountingRetry(1));
    }

    public BaseTestClient(long remoteServiceVersion) {
      this();
      mRemoteServiceVersion = remoteServiceVersion;
    }

    @Override
    protected ServiceType getRemoteServiceType() {
      return ServiceType.UNKNOWN_SERVICE;
    }

    @Override
    protected String getServiceName() {
      return SERVICE_NAME;
    }

    @Override
    protected long getServiceVersion() {
      return 1;
    }

    @Override
    protected long getRemoteServiceVersion() {
      return mRemoteServiceVersion;
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

  @Test
  public void unsupportedVersion() throws Exception {
    mExpectedException.expect(IOException.class);
    mExpectedException.expectMessage(INCOMPATIBLE_VERSION.getMessage(SERVICE_NAME, 0, 1));
    final AbstractClient client = new BaseTestClient(1);
    client.checkVersion(0);
    client.close();
  }

  @Test
  public void supportedVersion() throws Exception {
    final AbstractClient client = new BaseTestClient(1);
    client.checkVersion(1);
    client.close();
  }
}

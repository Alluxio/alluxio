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
import static org.junit.Assert.assertThrows;

import alluxio.conf.Configuration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ServiceType;
import alluxio.retry.CountingRetry;
import alluxio.security.user.BaseUserState;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit tests for {@link AbstractClient}.
 */
public final class AbstractClientTest {

  private static final InetSocketAddress TEST_SERVER_ADDRESS =
      new InetSocketAddress("localhost", 9999);

  private static final String SERVICE_NAME = "Test Service Name";
  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private static class BaseTestClient extends AbstractClient {
    private long mRemoteServiceVersion;

    protected BaseTestClient() {
      // Use no-retry policy for test
      super(ClientContext.create(Configuration.global()),
          () -> new CountingRetry(0));
    }

    protected BaseTestClient(ClientContext context) {
      super(context, () -> new CountingRetry(1));
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
    protected synchronized GrpcServerAddress queryGrpcServerAddress() throws UnavailableException {
      return GrpcServerAddress.create(TEST_SERVER_ADDRESS);
    }

    @Override
    protected long getRemoteServiceVersion() throws AlluxioStatusException {
      return mRemoteServiceVersion;
    }
  }

  private static class TestServiceNotFoundClient extends BaseTestClient {
    protected long getRemoteServiceVersion() throws AlluxioStatusException {
      throw new NotFoundException("Service not found");
    }

    @Override
    protected void beforeConnect() {
    }

    @Override
    protected void afterConnect() {
    }
  }

  @Test
  public void connectFailToDetermineMasterAddress() throws Exception {
    alluxio.Client client = new BaseTestClient() {
      @Override
      public synchronized GrpcServerAddress queryGrpcServerAddress() throws UnavailableException {
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

  @Test
  public void confAddress() throws Exception {
    ClientContext context = Mockito.mock(ClientContext.class);
    Mockito.when(context.getClusterConf()).thenReturn(
        Configuration.modifiableGlobal());

    InetSocketAddress baseAddress = new InetSocketAddress("0.0.0.0", 2000);
    InetSocketAddress confAddress = new InetSocketAddress("0.0.0.0", 2000);
    final alluxio.Client client = new BaseTestClient(context) {
      @Override
      protected synchronized GrpcServerAddress queryGrpcServerAddress() {
        return GrpcServerAddress.create(baseAddress);
      }

      @Override
      public synchronized String getRemoteHostName() {
        return baseAddress.getHostName();
      }

      @Override
      public synchronized InetSocketAddress getConfAddress() {
        return confAddress;
      }
    };

    ArgumentCaptor<InetSocketAddress> argument = ArgumentCaptor.forClass(InetSocketAddress.class);

    Mockito.doThrow(new RuntimeException("test"))
            .when(context)
            .loadConfIfNotLoaded(argument.capture());
    Mockito.doReturn(Mockito.mock(BaseUserState.class))
            .when(context)
            .getUserState();

    try {
      client.connect();
      Assert.fail();
    } catch (Exception e) {
      // ignore any exceptions. It's expected.
    }

    Assert.assertEquals(confAddress, argument.getValue());
  }

  @Test
  public void serviceNotFound() throws Exception {
    mExpectedException.expect(NotFoundException.class);
    final AbstractClient client = new TestServiceNotFoundClient();
    client.checkVersion(0);
    client.close();
  }

  @Test
  public void recoverAfterReLogin() throws Exception {
    try (AbstractClient client = new BaseTestClient(1) {
      int mAttemptCount = 0;
      @Override
      protected void beforeConnect() {
        // don't load configuration
      }

      @Override
      protected void doConnect() throws AlluxioStatusException {
        // fail for the first time and succeed in the second attempt
        mAttemptCount++;
        if (mAttemptCount == 1) {
          throw new UnauthenticatedException("Unauthenticated");
        }
        // dumb channel won't be used, just to avoid NPE
        mChannel = GrpcChannelBuilder.newBuilder(GrpcServerAddress.create(TEST_SERVER_ADDRESS),
                Configuration.global()).disableAuthentication().build();
      }
    }) {
      // Since re-login operation gets one free chance of retry,
      // this should succeed even if we use no-retry policy
      client.connect();
    }
  }

  @Test(timeout = 1000)
  public void noRetryForeverAfterReLogin() {
    try (AbstractClient client = new BaseTestClient(1) {
      @Override
      protected void beforeConnect() {
        // don't load configuration
      }

      @Override
      protected void doConnect() throws AlluxioStatusException {
        // throw UnauthenticatedException, connect shouldn't waste time
        // retrying forever
        throw new UnauthenticatedException("Unauthenticated");
      }
    }) {
      // should halt and error out
      assertThrows(UnauthenticatedException.class, client::connect);
    }
  }
}

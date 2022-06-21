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

package alluxio.security.authentication;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.GrpcChannelKey;
import alluxio.grpc.GrpcConnection;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.security.CurrentUser;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

/**
 * This class provides util methods for {@link AuthenticationUtils}s.
 */
@ThreadSafe
public final class AuthenticationUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUtils.class);
  private static final long AUTH_TIMEOUT = Configuration.getMs(
      PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT);

  /**
   * @param subject the subject to use (can be null)
   * @param conf Alluxio configuration
   * @return the configured impersonation user, or null if impersonation is not used
   */
  @Nullable
  public static String getImpersonationUser(Subject subject, AlluxioConfiguration conf) {
    // The user of the hdfs client
    String hdfsUser = null;

    if (subject != null) {
      // The HDFS client uses the subject to pass in the user
      Set<CurrentUser> user = subject.getPrincipals(CurrentUser.class);
      LOG.debug("Impersonation: subject: {}", subject);
      if (!user.isEmpty()) {
        hdfsUser = user.iterator().next().getName();
      }
    }

    // Determine the impersonation user
    String impersonationUser = null;
    if (conf.isSet(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME)) {
      impersonationUser = conf.getString(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME);
      LOG.debug("Impersonation: configured: {}", impersonationUser);
      if (Constants.IMPERSONATION_HDFS_USER.equals(impersonationUser)) {
        // Impersonate as the hdfs client user
        impersonationUser = hdfsUser;
      } else {
        // do not use impersonation, for any value that is not _HDFS_USER_
        if (impersonationUser != null && !impersonationUser.isEmpty()
            && !Constants.IMPERSONATION_NONE.equals(impersonationUser)) {
          LOG.warn("Impersonation ignored. Invalid configuration: {}", impersonationUser);
        }
        impersonationUser = null;
      }
      if (impersonationUser != null && impersonationUser.isEmpty()) {
        impersonationUser = null;
      }
    }
    LOG.debug("Impersonation: hdfsUser: {} impersonationUser: {}", hdfsUser, impersonationUser);
    return impersonationUser;
  }

  /**
   * It builds an authenticated channel.
   * @param connection Grpc connection
   * @param subject subject
   * @param authType authentication type
   * @return AuthenticatedChannelClientDriver
   */
  public static AuthenticatedChannelClientDriver authenticate(
      GrpcConnection connection, Subject subject, AuthType authType) throws AlluxioStatusException {
    SaslClientHandler clientHandler;
    switch (authType) {
      case SIMPLE:
      case CUSTOM:
        clientHandler =
            new alluxio.security.authentication.plain.SaslClientHandlerPlain(
                subject, Configuration.global());
        break;
      default:
        throw new UnauthenticatedException(
            String.format("Channel authentication scheme not supported: %s", authType.name()));
    }

    GrpcChannelKey channelKey = connection.getChannelKey();
    AuthenticatedChannelClientDriver authDriver;
    try {
      // Create client-side driver for establishing authenticated channel with the target.
      authDriver = new AuthenticatedChannelClientDriver(
          clientHandler, channelKey);
    } catch (SaslException t) {
      AlluxioStatusException e = AlluxioStatusException.fromThrowable(t);
      // Build a pretty message for authentication failure.
      String message = String.format(
          "Channel authentication failed with code:%s. Channel: %s, AuthType: %s, Error: %s",
          e.getStatusCode().name(), channelKey, authType, e);
      throw AlluxioStatusException
          .from(Status.fromCode(e.getStatusCode()).withDescription(message).withCause(t));
    }
    // Initialize client-server authentication drivers.
    SaslAuthenticationServiceGrpc.SaslAuthenticationServiceStub serverStub =
        SaslAuthenticationServiceGrpc.newStub(connection.getChannel());

    StreamObserver<SaslMessage> requestObserver = serverStub.authenticate(authDriver);
    authDriver.setServerObserver(requestObserver);

    // Start authentication with the target. (This is blocking.)
    authDriver.startAuthenticatedChannel(AUTH_TIMEOUT);

    // Intercept authenticated channel with channel-id injector.
    connection.interceptChannel(new ChannelIdInjector(channelKey.getChannelId()));
    return authDriver;
  }

  private AuthenticationUtils() {} // prevent instantiation
}

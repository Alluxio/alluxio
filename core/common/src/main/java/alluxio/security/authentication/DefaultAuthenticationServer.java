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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.security.authentication.plain.SaslServerHandlerPlain;
import alluxio.util.ThreadFactoryUtils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link AuthenticationServer}. Its functions include: -> Authentication
 * server against which client channels could get authenticated -> Registry for identity for known
 * channels during RPC calls.
 *
 */
@ThreadSafe
public class DefaultAuthenticationServer
    extends SaslAuthenticationServiceGrpc.SaslAuthenticationServiceImplBase
    implements AuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthenticationServer.class);

  /** List of channels authenticated against this server. */
  protected final ConcurrentHashMap<UUID, AuthenticatedChannelInfo> mChannels;
  /** Scheduler for periodic cleaning of channels registry. */
  protected final ScheduledExecutorService mScheduler;
  /** Address of the authentication host.  */
  protected final String mHostName;

  /** Interval for clean-up task to fire. */
  protected final long mCleanupIntervalMs;

  /** Alluxio client configuration. */
  protected final AlluxioConfiguration mConfiguration;

  /**
   * Creates {@link DefaultAuthenticationServer} instance.
   * @param hostName host name of the server
   * @param conf Alluxio client configuration
   */
  public DefaultAuthenticationServer(String hostName, AlluxioConfiguration conf) {
    mHostName = hostName;
    mConfiguration = conf;
    mCleanupIntervalMs =
            conf.getMs(PropertyKey.AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD);
    checkSupported(conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class));
    mChannels = new ConcurrentHashMap<>();
    mScheduler = Executors.newScheduledThreadPool(1,
        ThreadFactoryUtils.build("auth-cleanup", true));
    mScheduler.scheduleAtFixedRate(() -> {
      cleanupStaleClients();
    }, mCleanupIntervalMs, mCleanupIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public StreamObserver<SaslMessage> authenticate(StreamObserver<SaslMessage> responseObserver) {
    // Create and return server sasl driver that will coordinate authentication traffic.
    SaslStreamServerDriver driver = new SaslStreamServerDriver(this);
    driver.setClientObserver(responseObserver);
    return driver;
  }

  @Override
  public void registerChannel(UUID channelId, AuthenticatedUserInfo userInfo,
      SaslServer saslServer) {
    LOG.debug("Registering new channel:{} for user:{}", channelId, userInfo);
    if (null != mChannels.putIfAbsent(channelId,
        new AuthenticatedChannelInfo(userInfo, saslServer))) {
      AuthenticatedChannelInfo existingInfo = mChannels.remove(channelId);
      throw new RuntimeException(
          String.format("Channel: %s already exists in authentication registry for user: %s.",
              channelId.toString(), existingInfo.getUserInfo()));
    }
  }

  @Override
  public AuthenticatedUserInfo getUserInfoForChannel(UUID channelId)
      throws UnauthenticatedException {
    if (mChannels.containsKey(channelId)) {
      AuthenticatedChannelInfo clientInfo = mChannels.get(channelId);
      return clientInfo.getUserInfo();
    } else {
      throw new UnauthenticatedException(
          String.format("Client:%s needs to be authenticated", channelId.toString()));
    }
  }

  @Override
  public void unregisterChannel(UUID channelId) {
    AuthenticatedChannelInfo channelInfo = mChannels.remove(channelId);
    if (channelInfo != null) {
      try {
        channelInfo.getSaslServer().dispose();
      } catch (SaslException e) {
        LOG.warn("Failed to dispose sasl client for channel-Id: {}. Error: {}", channelId,
            e.getMessage());
      }
    }
  }

  @Override
  public SaslServerHandler createSaslHandler(ChannelAuthenticationScheme authScheme)
      throws SaslException, UnauthenticatedException {
    switch (authScheme) {
      case SIMPLE:
      case CUSTOM:
        return new SaslServerHandlerPlain(mHostName, mConfiguration);
      default:
        throw new StatusRuntimeException(Status.UNAUTHENTICATED.augmentDescription(
            String.format("Authentication scheme:%s is not supported", authScheme)));
    }
  }

  /**
   * Primitive that is invoked periodically for cleaning the registry from clients that has become
   * stale.
   */
  private void cleanupStaleClients() {
    LocalTime cleanupTime = LocalTime.now();
    LOG.debug("Starting cleanup authentication registry at {}", cleanupTime);
    // Get a list of stale clients under read lock.
    List<UUID> staleChannels = new ArrayList<>();
    for (Map.Entry<UUID, AuthenticatedChannelInfo> clientEntry : mChannels.entrySet()) {
      LocalTime lat = clientEntry.getValue().getLastAccessTime();
      if (lat.plusSeconds(mCleanupIntervalMs / 1000).isBefore(cleanupTime)) {
        staleChannels.add(clientEntry.getKey());
      }
    }

    // Unregister stale clients.
    LOG.debug("Found {} stale channels for cleanup.", staleChannels.size());
    for (UUID clientId : staleChannels) {
      unregisterChannel(clientId);
    }
    LOG.debug("Finished state channel cleanup at {}", LocalTime.now());
  }

  /**
   * Used to check if given authentication is supported by the server.
   *
   * @param authType authentication type
   * @throws RuntimeException if not supported
   */
  protected void checkSupported(AuthType authType) {
    switch (authType) {
      case NOSASL:
      case SIMPLE:
      case CUSTOM:
        return;
      default:
        throw new RuntimeException("Authentication type not supported:" + authType.name());
    }
  }

  /**
   * Represents a channel in authentication registry.
   * It's used internally to store and retrieve authentication principals
   * and Sasl objects per channel.
   */
  class AuthenticatedChannelInfo {
    private LocalTime mLastAccessTime;
    private SaslServer mAuthenticatedServer;
    private AuthenticatedUserInfo mUserInfo;

    /**
     * @param userInfo authenticated user info
     * @param authenticatedServer authenticated sasl server
     */
    public AuthenticatedChannelInfo(AuthenticatedUserInfo userInfo,
        SaslServer authenticatedServer) {
      mUserInfo = userInfo;
      mAuthenticatedServer = authenticatedServer;
      mLastAccessTime = LocalTime.now();
    }

    private synchronized void updateLastAccessTime() {
      mLastAccessTime = LocalTime.now();
    }

    /**
     * @return the last access time
     */
    public synchronized LocalTime getLastAccessTime() {
      return mLastAccessTime;
    }

    /**
     * PS: Updates the last-access-time for this instance.
     *
     * @return the sasl server
     */
    public SaslServer getSaslServer() {
      updateLastAccessTime();
      return mAuthenticatedServer;
    }

    /**
     * PS: Updates the last-access-time for this instance.
     *
     * @return the user name
     */
    public AuthenticatedUserInfo getUserInfo() {
      updateLastAccessTime();
      return mUserInfo;
    }
  }
}

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.resource.LockResource;
import alluxio.util.SecurityUtils;

import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default implementation of {@link AuthenticationServer}. Its functions include: -> Authentication
 * server against which client channels could get authenticated -> Registry for identity for known
 * channels during RPC calls.
 *
 */
public class DefaultAuthenticationServer
    extends SaslAuthenticationServiceGrpc.SaslAuthenticationServiceImplBase
    implements AuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthenticationServer.class);

  @GuardedBy("mClientsLock")
  /** List of channels authenticated against this server. */
  protected final Map<UUID, AuthenticatedChannelInfo> mChannels;
  /** Used to protect access to known channels. */
  protected final ReentrantReadWriteLock mClientsLock;
  /** Scheduler for periodic cleaning of channels registry. */
  protected final ScheduledExecutorService mScheduler;

  /** Interval for clean-up task to fire. */
  // TODO(gezer) make it configurable.
  protected final long mCleanupIntervalHour = 1L;

  /**
   * Creates {@link DefaultAuthenticationServer} instance.
   */
  public DefaultAuthenticationServer() {
    checkSupported(Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class));
    mChannels = new HashMap<>();
    mClientsLock = new ReentrantReadWriteLock(true);
    mScheduler = Executors.newScheduledThreadPool(1);
    mScheduler.scheduleAtFixedRate(() -> {
      cleanupStaleClients();
    }, mCleanupIntervalHour, mCleanupIntervalHour, TimeUnit.HOURS);
  }

  @Override
  public StreamObserver<SaslMessage> authenticate(StreamObserver<SaslMessage> responseObserver) {
    // Create and return server sasl driver that will coordinate authentication traffic.
    SaslStreamServerDriver driver = new SaslStreamServerDriver(this);
    driver.setClientObserver(responseObserver);
    return driver;
  }

  @Override
  public void registerChannel(UUID channelId, String authorizedUser, SaslServer saslServer) {
    try (LockResource clientsLockExclusive = new LockResource(mClientsLock.writeLock())) {
      if (mChannels.containsKey(channelId)) {
        throw new RuntimeException(String.format(
            "Channel: %s already exists in authentication registry.", channelId.toString()));
      }
      mChannels.put(channelId, new AuthenticatedChannelInfo(authorizedUser, saslServer));
      LOG.debug("Registered new channel:" + channelId);
    }
  }

  @Override
  public String getUserNameForChannel(UUID channelId) throws UnauthenticatedException {

    try (LockResource clientsLockShared = new LockResource(mClientsLock.readLock())) {
      if (mChannels.containsKey(channelId)) {
        AuthenticatedChannelInfo clientInfo = mChannels.get(channelId);
        return mChannels.get(channelId).getUserName();
      } else {
        throw new UnauthenticatedException(
            String.format("Client:%s needs to be authenticated", channelId.toString()));
      }
    }
  }

  @Override
  public void unregisterChannel(UUID channelId) {
    if (mChannels.containsKey(channelId)) {
      SaslServer serverToDispose = null;
      try (LockResource clientsLockExclusive = new LockResource(mClientsLock.writeLock())) {
        if (mChannels.containsKey(channelId)) {
          // Extract the Sasl server for disposing out of lock
          serverToDispose = mChannels.get(channelId).getSaslServer();
          mChannels.remove(channelId);
        }
      }
      if (serverToDispose != null) {
        try {
          serverToDispose.dispose();
        } catch (SaslException e) {
          LOG.warn("Failed to dispose sasl client for channel:" + channelId, e);
        }
      }
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
    try (LockResource clientsLockShared = new LockResource(mClientsLock.readLock())) {
      for (Map.Entry<UUID, AuthenticatedChannelInfo> clientEntry : mChannels.entrySet()) {
        LocalTime lat = clientEntry.getValue().getLastAccessTime();
        if (lat.plusHours(mCleanupIntervalHour).isBefore(cleanupTime)) {
          staleChannels.add(clientEntry.getKey());
        }
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
  private void checkSupported(AuthType authType) {
    switch (authType) {
      case NOSASL:
      case SIMPLE:
      case CUSTOM:
        return;
      default:
        throw new RuntimeException("Authentication type not supported:" + authType.name());
    }
  }

  @Override
  public List<ServerInterceptor> getInterceptors() {
    if (!SecurityUtils.isSecurityEnabled()) {
      return Collections.emptyList();
    }
    List<ServerInterceptor> interceptorsList = new ArrayList<>();
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    checkSupported(authType);
    switch (authType) {
      case SIMPLE:
      case CUSTOM:
        interceptorsList.add(new AuthenticatedUserInjector(this));
        break;
      default:
        throw new RuntimeException("Unsupported authentication type:" + authType);
    }
    return interceptorsList;
  }

  /**
   * Represents a channel in authentication registry.
   */
  class AuthenticatedChannelInfo {
    private LocalTime mLastAccessTime;
    private SaslServer mAuthenticatedServer;
    private String mAuthorizedUser;

    /**
     * @param authorizedUser authorized user
     * @param authenticatedServer authenticated sasl server
     */
    public AuthenticatedChannelInfo(String authorizedUser, SaslServer authenticatedServer) {
      mAuthorizedUser = authorizedUser;
      mAuthenticatedServer = authenticatedServer;
      mLastAccessTime = LocalTime.now();
    }

    private void updateLastAccessTime() {
      synchronized (mLastAccessTime) {
        mLastAccessTime = LocalTime.now();
      }
    }

    /**
     * @return the sasl server
     */
    public SaslServer getSaslServer() {
      updateLastAccessTime();
      return mAuthenticatedServer;
    }

    /**
     * @return the user name
     */
    public String getUserName() {
      updateLastAccessTime();
      return mAuthorizedUser;
    }

    /**
     * @return the last access time
     */
    public LocalTime getLastAccessTime() {
      return mLastAccessTime;
    }
  }
}

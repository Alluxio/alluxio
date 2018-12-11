package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.resource.LockResource;
import alluxio.util.SecurityUtils;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import net.jcip.annotations.GuardedBy;

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

import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

public class DefaultAuthenticationServer extends
    AlluxioSaslClientServiceGrpc.AlluxioSaslClientServiceImplBase implements AuthenticationServer {
  @GuardedBy("mClientsLock")
  protected final Map<UUID, AuthenticatedClientInfo> mClients;
  protected final ReentrantReadWriteLock mClientsLock;
  protected final ScheduledExecutorService mScheduler;

  // TODO(gezer) configurable
  protected final long mCleanupIntervalHour = 1L;

  public DefaultAuthenticationServer() {
    checkSupported(Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class));
    mClients = new HashMap<>();
    mClientsLock = new ReentrantReadWriteLock(true);
    mScheduler = Executors.newScheduledThreadPool(1);
    mScheduler.scheduleAtFixedRate(() -> {
      cleanupStaleClients();
    }, mCleanupIntervalHour, mCleanupIntervalHour, TimeUnit.HOURS);
  }

  @Override
  public StreamObserver<SaslMessage> authenticate(StreamObserver<SaslMessage> responseObserver) {
    SaslStreamServerDriver driver = new SaslStreamServerDriver(this);
    driver.setClientObserver(responseObserver);
    return driver;
  }

  public void registerClient(UUID clientId, String authorizedUser, SaslServer saslServer) {
    try (LockResource clientsLockExclusive = new LockResource(mClientsLock.writeLock())) {
      if (mClients.containsKey(clientId)) {
        throw new RuntimeException(String
            .format("Client: %s already exists in authentication registry.", clientId.toString()));
      }
      mClients.put(clientId, new AuthenticatedClientInfo(authorizedUser, saslServer));
    }
  }

  public String getUserNameForClient(UUID clientId) throws UnauthenticatedException {

    try (LockResource clientsLockShared = new LockResource(mClientsLock.readLock())) {
      if (mClients.containsKey(clientId)) {
        AuthenticatedClientInfo clientInfo = mClients.get(clientId);
        return mClients.get(clientId).getUserName();
      } else {
        throw new UnauthenticatedException(
            String.format("Client:%s needs to be authenticated", clientId.toString()));
      }
    }
  }

  public void unregisterClient(UUID clientId) {
    if (mClients.containsKey(clientId)) {
      SaslServer serverToDispose = null;
      try (LockResource clientsLockExclusive = new LockResource(mClientsLock.writeLock())) {
        if (mClients.containsKey(clientId)) {
          // Extract the Sasl server for disposing out of lock
          serverToDispose = mClients.get(clientId).getSaslServer();
          mClients.remove(clientId);
        }
      }
      if (serverToDispose != null) {
        try {
          serverToDispose.dispose();
        } catch (SaslException e) {
          // TODO(ggezer) log.
        }
      }
    }
  }

  private void cleanupStaleClients() {
    LocalTime cleanupTime = LocalTime.now();
    List<UUID> staleClients = new ArrayList<>();
    try (LockResource clientsLockShared = new LockResource(mClientsLock.readLock())) {
      for (Map.Entry<UUID, AuthenticatedClientInfo> clientEntry : mClients.entrySet()) {
        LocalTime lat = clientEntry.getValue().getLastAccessTime();
        if (lat.plusHours(mCleanupIntervalHour).isBefore(cleanupTime)) {
          staleClients.add(clientEntry.getKey());
        }
      }
    }
    for (UUID clientId : staleClients) {
      unregisterClient(clientId);
    }
  }

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
    }
    return interceptorsList;
  }

  class AuthenticatedClientInfo {
    private LocalTime mLastAccessTime;
    private SaslServer mAuthenticatedServer;
    private String mAuthorizedUser;

    public AuthenticatedClientInfo(String authorizedUser, SaslServer authenticatedServer) {
      mAuthorizedUser = authorizedUser;
      mAuthenticatedServer = authenticatedServer;
      mLastAccessTime = LocalTime.now();
    }

    private void updateLastAccessTime() {
      synchronized (mLastAccessTime) {
        mLastAccessTime = LocalTime.now();
      }
    }

    public SaslServer getSaslServer() {
      updateLastAccessTime();
      return mAuthenticatedServer;
    }

    public String getUserName() {
      updateLastAccessTime();
      return mAuthorizedUser;
    }

    public LocalTime getLastAccessTime() {
      return mLastAccessTime;
    }
  }
}

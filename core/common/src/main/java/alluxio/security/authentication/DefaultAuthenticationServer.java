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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

public class DefaultAuthenticationServer extends
    AlluxioSaslClientServiceGrpc.AlluxioSaslClientServiceImplBase implements AuthenticationServer {
  @GuardedBy("mClientsLock")
  private Map<UUID, AuthenticatedClientInfo> mClients;
  private ReentrantReadWriteLock mClientsLock;

  // TODO(ggezer) Launch a periodic task for cleaning the clients

  public DefaultAuthenticationServer() {
    mClients = new HashMap<>();
    mClientsLock = new ReentrantReadWriteLock(true);
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

  public List<ServerInterceptor> getInterceptors() {
    if (!SecurityUtils.isSecurityEnabled()) {
      return Collections.emptyList();
    }
    List<ServerInterceptor> interceptorsList = new ArrayList<>();
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    switch (authType) {
      case SIMPLE:
      case CUSTOM:
        interceptorsList.add(new AuthenticatedUserInjector(this));
        break;
      default:
        throw new RuntimeException(
            String.format("Authentication type:%s not supported", authType.name()));
    }
    return interceptorsList;
  }

  class AuthenticatedClientInfo {
    private SaslServer mAuthenticatedServer;
    private String mAuthorizedUser;

    public AuthenticatedClientInfo(String authorizedUser, SaslServer authenticatedServer) {
      mAuthorizedUser = authorizedUser;
      mAuthenticatedServer = authenticatedServer;
    }

    public SaslServer getSaslServer() {
      return mAuthenticatedServer;
    }

    public String getUserName() {
      return mAuthorizedUser;
    }
  }
}

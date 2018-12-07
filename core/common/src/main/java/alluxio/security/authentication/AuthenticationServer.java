package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import io.grpc.ServerInterceptor;

import javax.security.sasl.SaslServer;
import java.util.List;
import java.util.UUID;

public interface AuthenticationServer {
  public void registerClient(UUID clientId, String authorizedUser, SaslServer saslServer);

  public String getUserNameForClient(UUID clientId) throws UnauthenticatedException;

  public void unregisterClient(UUID clientId);

  public List<ServerInterceptor> getInterceptors();
}

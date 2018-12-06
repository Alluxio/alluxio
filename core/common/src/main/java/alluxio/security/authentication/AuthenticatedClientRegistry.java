package alluxio.security.authentication;

import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnauthenticatedException;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class AuthenticatedClientRegistry {
  private ConcurrentHashMap<UUID, AuthenticatedClient> mClients;

  public AuthenticatedClientRegistry() {
    mClients = new ConcurrentHashMap<>();
  }

  public void registerClient(UUID clientId, AuthenticatedClient clientInfo) {
    if (mClients.containsKey(clientId)) {
      throw new RuntimeException(String
          .format("Client: %s already exists in authentication registry.", clientId.toString()));
    }
    mClients.put(clientId, clientInfo);
  }

  public String getUserNameForClient(UUID clientId) throws UnauthenticatedException {
    AuthenticatedClient clientInfo = mClients.get(clientId);
    if (clientInfo != null) {
      return clientInfo.getUserName();
    } else {
      throw new UnauthenticatedException(String
          .format("Could not found client:%s in authentication registry", clientId.toString()));
    }
  }

  public void unregisterClient(UUID clientId) {
    try {
      AuthenticatedClient clientInfo = mClients.get(clientId);
      if(clientInfo != null) {
        SaslServer saslServer = clientInfo.getSaslServer();
        if(saslServer != null) {
          saslServer.dispose();
        }
      }
    } catch (SaslException e) {
      // TODO(ggezer) log.
    }

    mClients.remove(clientId);
  }
}

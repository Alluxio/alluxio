package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import javax.security.sasl.SaslServer;
import java.util.List;
import java.util.UUID;

/**
 * Interface for authentication server implementations.
 */
public interface AuthenticationServer extends BindableService {
  /**
   * Registers new user against given channel.
   *
   * @param channelId channel id
   * @param authorizedUser authorized user name
   * @param saslServer server that has been used for authentication
   */
  public void registerChannel(UUID channelId, String authorizedUser, SaslServer saslServer);

  /**
   * @param channelId channel id
   * @return user name associated with the given channel
   * @throws UnauthenticatedException if given channel is not registered
   */
  public String getUserNameForChannel(UUID channelId) throws UnauthenticatedException;

  /**
   * Unregisters given channel.
   *
   * @param channelId channel id
   */
  public void unregisterChannel(UUID channelId);

  /**
   * @return list of server-side interceptors that are required for configured authentication type
   */
  public List<ServerInterceptor> getInterceptors();
}

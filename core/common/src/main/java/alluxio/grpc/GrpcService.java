package alluxio.grpc;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;

/**
 * Utility class for wrapping gRPC service definition. It's internally used to specify whether the
 * service requires an authenticated client or not.
 */
public class GrpcService {
  /** internal service definition instance. */
  private final ServerServiceDefinition mServiceDefinition;
  /** whether this service should be accessed with authentication. */
  private boolean mAuthenticated = true;

  /**
   * Creates a new {@link GrpcService}.
   *
   * @param bindableService gRPC bindable service
   */
  public GrpcService(BindableService bindableService) {
    mServiceDefinition = bindableService.bindService();
  }

  /**
   * Creates a new {@link GrpcService}.
   *
   * @param serviceDefinition gRPC service definition
   */
  public GrpcService(ServerServiceDefinition serviceDefinition) {
    mServiceDefinition = serviceDefinition;
  }

  /**
   * If called, clients can access this service's methods without authentication.
   * 
   * @return the updated {@link GrpcService} instance
   */
  public GrpcService disableAuthentication() {
    mAuthenticated = false;
    return this;
  }

  /**
   * @return {@code true} if this service should be accessed with authentication
   */
  public boolean isAuthenticated() {
    return mAuthenticated;
  }

  /**
   * @return the internal {@link ServerServiceDefinition}
   */
  public ServerServiceDefinition getServiceDefinition() {
    return mServiceDefinition;
  }
}

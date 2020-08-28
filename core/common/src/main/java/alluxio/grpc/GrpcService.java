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

package alluxio.grpc;

import com.google.common.io.Closer;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;

import java.io.Closeable;

/**
 * Utility class for wrapping gRPC service definition. It's internally used to specify whether the
 * service requires an authenticated client or not.
 */
public class GrpcService {
  /** internal service definition instance. */
  private final ServerServiceDefinition mServiceDefinition;
  /** whether this service should be accessed with authentication. */
  private boolean mAuthenticated = true;
  /** closer to be closed before owning server is shut down. */
  private Closer mCloser = Closer.create();

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
   * Add a new closeable resource to this service's closer.
   *
   * Registered closer will be owned and closed by {@link GrpcServer} that hosts
   * the service.
   *
   * @param closeable the closeable resource
   * @return the updated {@link GrpcService} instance
   */
  public GrpcService withCloseable(Closeable closeable) {
    mCloser.register(closeable);
    return this;
  }

  /**
   * Gets the closer associated with this service.
   *
   * This is mostly used to close streams associated with service to not stall
   * server's graceful shutdown.
   *
   * @return closer for the service
   */
  public Closer getCloser() {
    return mCloser;
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

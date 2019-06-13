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

package alluxio;

import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Interface representing an Alluxio server.
 *
 * @param <T> type of the start options
 */
public interface Server<T> {

  /**
   * @return the server dependencies
   */
  Set<Class<? extends Server>> getDependencies();

  /**
   * @return the server's name
   */
  String getName();

  /**
   * @return a map from service names to gRPC serviced that serve RPCs for this server
   */
  Map<ServiceType, GrpcService> getServices();

  /**
   * Starts the Alluxio server.
   *
   * @param options the start options
   */
  void start(T options) throws IOException;

  /**
   * Stops the Alluxio server. Here, anything created or started in {@link #start(T)} should be
   * cleaned up and shutdown.
   */
  void stop() throws IOException;

  /**
   * Closes the server.
   */
  void close() throws IOException;
}

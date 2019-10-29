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

package alluxio.master.backup;

import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for interacting with backup roles.
 */
public interface BackupRole extends BackupOps {

  /**
   * Starts the role.
   *
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Stops the role.
   *
   * @throws IOException
   */
  void stop() throws IOException;

  /**
   * @return services for the role
   */
  Map<ServiceType, GrpcService> getRoleServices();
}

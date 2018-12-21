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

package alluxio.testutils;

/**
 * Constants for integration tests.
 */
public class IntegrationTestConstants {
  // DataServer variations.
  public static final String GRPC_DATA_SERVER = "alluxio.worker.grpc.GrpcDataServer";

  // Netty transfer types.
  public static final String MAPPED_TRANSFER = "MAPPED";
  public static final String FILE_CHANNEL_TRANSFER = "TRANSFER";
  public static final String UNUSED_TRANSFER = "UNUSED";
}

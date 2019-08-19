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

package alluxio.metrics;

/**
 * Metrics of an Alluxio client.
 */
public final class ClientMetrics {
  /** Total number of bytes short-circuit read from local storage. */
  public static final String BYTES_READ_LOCAL = "BytesReadLocal";
  public static final String BYTES_READ_LOCAL_THROUGHPUT = "BytesReadLocalThroughput";
  public static final String BYTES_WRITTEN_UFS = "BytesWrittenUfs";

  private ClientMetrics() {} // prevent instantiation
}

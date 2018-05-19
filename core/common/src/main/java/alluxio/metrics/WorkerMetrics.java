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
 * Metrics of an Alluxio worker.
 */
public final class WorkerMetrics {

  /**
   * Total number of bytes read from Alluxio storage through this worker. This does not include UFS
   * reads.
   */
  public static final String BYTES_READ_ALLUXIO = "BytesReadAlluxio";
  public static final String BYTES_READ_ALLUXIO_THROUGHPUT = "BytesReadAlluxioThroughput";

  /** Total number of bytes read from UFS through this worker. */
  public static final String BYTES_READ_UFS = "BytesReadUfs";
  public static final String BYTES_READ_UFS_THROUGHPUT = "BytesReadUfsThroughput";

  public static final String UFS = "UFS";

  private WorkerMetrics() {} // prevent instantiation
}

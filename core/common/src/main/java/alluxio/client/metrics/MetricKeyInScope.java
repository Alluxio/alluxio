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

package alluxio.client.metrics;

/**
 * Metric keys of each scope.
 */
public enum MetricKeyInScope {
  /** Bytes stored in cache.*/
  BYTES_IN_CACHE,
  /** Cache hits. Bytes read from cache.*/
  BYTES_READ_CACHE,
  /** Bytes read from external, may be larger than requests due to reading complete pages.*/
  BYTES_READ_EXTERNAL;
}

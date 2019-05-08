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

package alluxio.client.meta;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MetricValue;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Interface for a meta master client.
 */
public interface MetaMasterClient extends Closeable {
  /**
   * Writes a backup of the journal to the specified directory. The backup is written to the
   * directory with a file name containing the date when the file was written.
   *
   * @param options backup options
   * @return the server response
   */
  BackupResponse backup(BackupPOptions options) throws IOException;

  /**
   * Gets the server-side configuration check report.
   *
   * @return configuration check report
   */
  ConfigCheckReport getConfigReport() throws IOException;

  /**
   * @param masterInfoFields optional list of fields to query; if null all fields will be queried
   * @return the requested master info
   */
  MasterInfo getMasterInfo(Set<MasterInfoField> masterInfoFields) throws IOException;

  /**
   * Gets a map of metrics property names and their values from metrics system.
   *
   * @return a map of metrics information
   */
  Map<String, MetricValue> getMetrics() throws AlluxioStatusException;

  /**
   * Creates a checkpoint in the primary master journal system.
   *
   * @return the hostname of the master that did the checkpoint
   */
  String checkpoint() throws IOException;
}

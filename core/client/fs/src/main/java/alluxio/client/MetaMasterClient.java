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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.wire.ConfigProperty;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;
import alluxio.wire.MetricValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for a meta master client.
 */
public interface MetaMasterClient extends Closeable {
  /**
   * Exports a snapshot of the journal to the specified directory URI. The snapshot is written
   * to the directory with a file name containing the date when the file was written.
   *
   * @param uri the URI of the directory to export to
   *            @return the
   */
  String exportJournal(AlluxioURI uri) throws IOException;

  /**
   * Gets the runtime configuration information.
   *
   * @return a list of configuration information
   */
  List<ConfigProperty> getConfiguration() throws IOException;

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
}

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

package alluxio.cli;

/**
 * The validation tool factory interface.
 */
public interface ValidationToolFactory {

  /**
   * Creates a new instance of an {@link ValidationTool}.
   * Creation must not interact with external services.
   *
   * @param metastoreUri hive metastore uris
   * @param database database to run tests against
   * @param tables tables to run tests against
   * @param socketTimeout socket time of hms operations
   * @return a new validation tool instance
   */
  ValidationTool create(String metastoreUri, String database, String tables, int socketTimeout);
}

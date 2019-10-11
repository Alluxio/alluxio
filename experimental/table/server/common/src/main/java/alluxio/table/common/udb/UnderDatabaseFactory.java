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

package alluxio.table.common.udb;

/**
 * The under database factory interface.
 */
public interface UnderDatabaseFactory {

  /**
   * @return the type of under database for the factory
   */
  String getType();

  /**
   * Creates a new instance of the udb. Creation must not interact with external services.
   *
   * @param udbContext the db context
   * @param configuration configuration values
   * @return a new instance of the under database
   */
  UnderDatabase create(UdbContext udbContext, UdbConfiguration configuration);
}

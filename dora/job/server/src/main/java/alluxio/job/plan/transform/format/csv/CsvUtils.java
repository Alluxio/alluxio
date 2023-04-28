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

package alluxio.job.plan.transform.format.csv;

import alluxio.job.plan.transform.HiveConstants;

/**
 * Utilities for implementing csv reader and writer.
 */
public class CsvUtils {
  private CsvUtils() {} // Prevents initialization

  /**
   * @param type the type of a field
   * @return whether the field has different types in read and write schema
   */
  public static boolean isReadWriteTypeInconsistent(String type) {
    return type.startsWith(HiveConstants.Types.DECIMAL)
        || type.equals(HiveConstants.Types.BINARY)
        || type.equals(HiveConstants.Types.DATE)
        || type.equals(HiveConstants.Types.TIMESTAMP);
  }
}

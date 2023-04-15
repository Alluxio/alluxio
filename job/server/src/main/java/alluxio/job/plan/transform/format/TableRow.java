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

package alluxio.job.plan.transform.format;

import alluxio.job.plan.transform.format.parquet.ParquetRow;

import java.io.IOException;

/**
 * A row in a table.
 */
public interface TableRow {
  /**
   * @return the row in parquet representation
   * @throws IOException when failed to transform to parquet
   */
  ParquetRow toParquet() throws IOException;

  /**
   * @param column the column
   * @return the column value
   */
  Object getColumn(String column);
}

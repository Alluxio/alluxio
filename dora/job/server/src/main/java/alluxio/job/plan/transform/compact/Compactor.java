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

package alluxio.job.plan.transform.compact;

import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableWriter;
import alluxio.job.plan.transform.format.tables.TablesWriter;

import java.io.IOException;
import java.util.List;

/**
 * Compacts a list of inputs to an output.
 * The output can either be a single table file, or a group of table files where each file is
 * completed once it reaches a certain size.
 */
public interface Compactor {
  /**
   * Compacts a list of inputs to the output.
   * Closing the readers and writer is the responsibility of the caller.
   *
   * @param inputs a list of table readers
   * @param output a table writer, can be a {@link TablesWriter}
   * @throws IOException when compaction fails
   */
  void compact(List<TableReader> inputs, TableWriter output) throws IOException;
}

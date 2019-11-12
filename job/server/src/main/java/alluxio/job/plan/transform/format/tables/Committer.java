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

package alluxio.job.plan.transform.format.tables;

import alluxio.job.plan.transform.format.TableWriter;

/**
 * Determines when to commit/complete a table file when writing a stream of rows to a group of
 * table files.
 * Designed to be used in {@link TablesWriter}.
 */
public interface Committer {
  /**
   * @param writer the current writer containing statistics about the data that has been written
   * @return whether the current table file should commit/complete
   */
  boolean shouldCommit(TableWriter writer);
}

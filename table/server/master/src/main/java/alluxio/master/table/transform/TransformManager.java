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

package alluxio.master.table.transform;

import alluxio.master.table.AlluxioCatalog;
import alluxio.table.common.transform.TransformPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

// TODO(cc): journal state
/**
 * Manages transformations.
 */
public class TransformManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransformManager.class);

  private final AlluxioCatalog mCatalog;

  /**
   * Creates an instance.
   *
   * @param catalog the catalog
   */
  public TransformManager(AlluxioCatalog catalog) {
    mCatalog = catalog;
  }

  /**
   * Executes the plans for the table transformation.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param plans the plans to execute for the transformation
   */
  public void execute(String dbName, String tableName, List<TransformPlan> plans) {
    // TODO(cc): implement
  }
}

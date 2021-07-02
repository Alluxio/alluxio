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

package alluxio.table.under.hive;

import alluxio.table.common.Layout;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.layout.HiveLayout;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive table implementation.
 */
public class HivePartition implements UdbPartition {
  private static final Logger LOG = LoggerFactory.getLogger(HivePartition.class);

  private final HiveLayout mLayout;

  /**
   * Creates an instance.
   *
   * @param layout the layout
   */
  public HivePartition(HiveLayout layout) {
    mLayout = layout;
  }

  @Override
  public String getSpec() {
    return mLayout.getSpec();
  }

  @Override
  public Layout getLayout() {
    return mLayout;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Spec", getSpec())
        .add("Values", mLayout.getData().getValuesList())
        .add("PartitionName", mLayout.getData().getPartitionName())
        .add("DatabaseName", mLayout.getData().getDbName())
        .add("TableName", mLayout.getData().getTableName())
        .add("Parameters", mLayout.getData().getParametersMap())
        .toString();
  }
}

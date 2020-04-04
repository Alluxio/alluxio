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

package alluxio.table.under.glue;

import alluxio.table.common.Layout;
import alluxio.table.common.UdbPartition;

import alluxio.table.common.layout.HiveLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Glue partition implementation.
 */
public class GluePartition implements UdbPartition {
  private static final Logger LOG = LoggerFactory.getLogger(GluePartition.class);

  private final HiveLayout mLayout;

  /**
   * Create Glue partition instance.
   *
   * @param layout glue table layout
   */
  public GluePartition(HiveLayout layout) {
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
}

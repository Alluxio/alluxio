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

package alluxio.master.catalog;

import alluxio.grpc.catalog.PartitionSpec;
import alluxio.table.common.Layout;
import alluxio.table.common.UdbPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The table partition class.
 */
public class Partition {
  private static final Logger LOG = LoggerFactory.getLogger(Partition.class);

  private final String mPartitionSpec;
  private final Layout mBaseLayout;
  private Layout mTransformedLayout;

  /**
   * Creates an instance.
   *
   * @param partitionSpec the partition spec
   * @param baseLayout the partition layout
   */
  public Partition(String partitionSpec, Layout baseLayout) {
    mPartitionSpec = partitionSpec;
    mBaseLayout = baseLayout;
  }

  /**
   * Creates an instance from a udb partition.
   *
   * @param udbPartition the udb partition
   */
  public Partition(UdbPartition udbPartition) {
    this(udbPartition.getSpec(), udbPartition.getLayout());
  }

  /**
   * @return the base layout
   */
  public Layout getBaseLayout() {
    return mBaseLayout;
  }

  /**
   * Transforms the base layout to a new type of layout at a new location.
   *
   * @param type the new type of layout
   * @param location the new location of the transformed partition
   * @throws IOException when failed to transform to the specified type of layout
   */
  public synchronized void transformLayout(String type, String location) throws IOException {
    LOG.info("Transform layout of type = " + type + " with location = " + location);
    mTransformedLayout = mBaseLayout.transform(type, location);
  }

  /**
   * @return the proto representation
   */
  public synchronized alluxio.grpc.catalog.Partition toProto() {
    Layout layout = mTransformedLayout == null ? mBaseLayout : mTransformedLayout;
    LOG.info("Layout location = " + layout.getLocation());
    return alluxio.grpc.catalog.Partition.newBuilder()
        .setPartitionSpec(PartitionSpec.newBuilder().setSpec(mPartitionSpec).build())
        .setLayout(layout.toProto())
        .build();
  }
}

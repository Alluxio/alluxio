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

package alluxio.master.table;

import alluxio.grpc.table.PartitionSpec;
import alluxio.table.common.Layout;
import alluxio.table.common.LayoutRegistry;
import alluxio.table.common.UdbPartition;

/**
 * The table partition class.
 */
public class Partition {
  private final String mPartitionSpec;
  private final Layout mBaseLayout;

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
   * Returns the base layout.
   *
   * @return base layout
   */
  public Layout getLayout() {
    return mBaseLayout;
  }

  /**
   * @return the proto representation
   */
  public alluxio.grpc.table.Partition toProto() {
    return alluxio.grpc.table.Partition.newBuilder()
        .setPartitionSpec(PartitionSpec.newBuilder().setSpec(mPartitionSpec).build())
        .setLayout(mBaseLayout.toProto())
        .build();
  }

  /**
   * @param layoutRegistry the layout registry
   * @param proto the proto representation
   * @return the java representation
   */
  public static Partition fromProto(LayoutRegistry layoutRegistry,
      alluxio.grpc.table.Partition proto) {
    return new Partition(proto.getPartitionSpec().getSpec(),
        layoutRegistry.create(proto.getLayout()));
  }
}

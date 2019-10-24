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
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;

import java.io.IOException;

/**
 * The table partition class.
 */
public class Partition {
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
   * @return the current layout
   */
  public synchronized Layout getLayout() {
    return mTransformedLayout == null ? mBaseLayout : mTransformedLayout;
  }

  /**
   * Sets the transformed layout.
   *
   * @param layout the transformed layout
   */
  public synchronized void setTransformedLayout(Layout layout) {
    mTransformedLayout = layout;
  }

  /**
   * @return the partition speck
   */
  public String getSpec() {
    return mPartitionSpec;
  }

  /**
   * Returns a plan to transform this partition.
   *
   * @param transformContext the {@link TransformContext}
   * @param definition the transformation definition
   * @return the transformation plan
   */
  public TransformPlan getTransformPlan(TransformContext transformContext,
      TransformDefinition definition) throws IOException {
    return mBaseLayout.getTransformPlan(transformContext, definition);
  }

  /**
   * @return the proto representation
   */
  public alluxio.grpc.table.Partition toProto() {
    return alluxio.grpc.table.Partition.newBuilder()
        .setPartitionSpec(PartitionSpec.newBuilder().setSpec(mPartitionSpec).build())
        .setLayout(getLayout().toProto())
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

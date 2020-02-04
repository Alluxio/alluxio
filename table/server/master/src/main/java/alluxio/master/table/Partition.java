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
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.util.List;

/**
 * The table partition class.
 */
public class Partition {
  private static final long FIRST_VERSION = 1;

  private final String mPartitionSpec;
  private final Layout mBaseLayout;
  private final long mVersion;
  private final long mVersionCreationTime;
  private volatile Transformation mTransformation;

  /**
   * Information kept for the latest transformation on the partition.
   */
  private static final class Transformation {
    /** The definition of the transformation. */
    private String mDefinition;
    /** The transformed layout. */
    private Layout mLayout;

    /**
     * @param definition the transformation definition
     * @param layout the transformed layout
     */
    public Transformation(String definition, Layout layout) {
      mDefinition = definition;
      mLayout = layout;
    }

    /**
     * @return the transformation definition
     */
    public String getDefinition() {
      return mDefinition;
    }

    /**
     * @return the transformed layout
     */
    public Layout getLayout() {
      return mLayout;
    }

    /**
     * @return the proto representation
     */
    public alluxio.grpc.table.Transformation toProto() {
      return alluxio.grpc.table.Transformation.newBuilder()
          .setDefinition(mDefinition)
          .setLayout(mLayout.toProto())
          .build();
    }

    /**
     * @param layoutRegistry the layout registry
     * @param proto the proto representation
     * @return the java representation
     */
    public static Transformation fromProto(LayoutRegistry layoutRegistry,
        alluxio.grpc.table.Transformation proto) {
      return new Transformation(proto.getDefinition(), layoutRegistry.create(proto.getLayout()));
    }
  }

  /**
   * Creates an instance.
   *
   * @param partitionSpec the partition spec
   * @param baseLayout the partition layout
   * @param version the version
   * @param versionCreationTime the version creation time
   */
  private Partition(String partitionSpec, Layout baseLayout, long version,
      long versionCreationTime) {
    mPartitionSpec = partitionSpec;
    mBaseLayout = baseLayout;
    mVersion = version;
    mVersionCreationTime = versionCreationTime;
  }

  /**
   * Creates an instance from a udb partition.
   *
   * @param udbPartition the udb partition
   */
  public Partition(UdbPartition udbPartition) {
    this(udbPartition.getSpec(), udbPartition.getLayout(), FIRST_VERSION,
        CommonUtils.getCurrentMs());
  }

  /**
   * Creates the next version of an existing partition.
   *
   * @param udbPartition the udb partition
   * @return a new Partition instance representing the next version of this partition
   */
  public Partition createNext(UdbPartition udbPartition) {
    return new Partition(udbPartition.getSpec(), udbPartition.getLayout(), getVersion() + 1,
        CommonUtils.getCurrentMs());
  }

  /**
   * @return the current layout
   */
  public Layout getLayout() {
    return mTransformation == null ? mBaseLayout : mTransformation.getLayout();
  }

  /**
   * @return the base layout
   */
  public Layout getBaseLayout() {
    return mBaseLayout;
  }

  /**
   * @return the version
   */
  public long getVersion() {
    return mVersion;
  }

  /**
   * Transform the partition.
   *
   * @param definition the transformation definition
   * @param layout the transformed layout
   */
  public void transform(String definition, Layout layout) {
    mTransformation = new Transformation(definition, layout);
  }

  /**
   * @param definition the transformation definition
   * @return whether the latest transformation of Partition has the same definition
   */
  public boolean isTransformed(String definition) {
    return mTransformation != null
        && mTransformation.getDefinition().equals(definition);
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
    alluxio.grpc.table.Partition.Builder builder = alluxio.grpc.table.Partition.newBuilder()
        .setPartitionSpec(PartitionSpec.newBuilder().setSpec(mPartitionSpec).build())
        .setBaseLayout(mBaseLayout.toProto())
        .setVersion(mVersion)
        .setVersionCreationTime(mVersionCreationTime);
    if (mTransformation != null) {
      builder.addTransformations(mTransformation.toProto());
    }
    return builder.build();
  }

  /**
   * @param layoutRegistry the layout registry
   * @param proto the proto representation
   * @return the java representation
   */
  public static Partition fromProto(LayoutRegistry layoutRegistry,
      alluxio.grpc.table.Partition proto) {
    Partition partition = new Partition(proto.getPartitionSpec().getSpec(),
        layoutRegistry.create(proto.getBaseLayout()), proto.getVersion(),
        proto.getVersionCreationTime());
    List<alluxio.grpc.table.Transformation> transformations = proto.getTransformationsList();
    if (!transformations.isEmpty()) {
      partition.mTransformation = Transformation.fromProto(layoutRegistry, transformations.get(0));
    }
    return partition;
  }
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface PartitionOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.Partition)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.table.PartitionSpec partition_spec = 1;</code>
   * @return Whether the partitionSpec field is set.
   */
  boolean hasPartitionSpec();
  /**
   * <code>optional .alluxio.grpc.table.PartitionSpec partition_spec = 1;</code>
   * @return The partitionSpec.
   */
  alluxio.grpc.table.PartitionSpec getPartitionSpec();
  /**
   * <code>optional .alluxio.grpc.table.PartitionSpec partition_spec = 1;</code>
   */
  alluxio.grpc.table.PartitionSpecOrBuilder getPartitionSpecOrBuilder();

  /**
   * <code>optional .alluxio.grpc.table.Layout base_layout = 2;</code>
   * @return Whether the baseLayout field is set.
   */
  boolean hasBaseLayout();
  /**
   * <code>optional .alluxio.grpc.table.Layout base_layout = 2;</code>
   * @return The baseLayout.
   */
  alluxio.grpc.table.Layout getBaseLayout();
  /**
   * <code>optional .alluxio.grpc.table.Layout base_layout = 2;</code>
   */
  alluxio.grpc.table.LayoutOrBuilder getBaseLayoutOrBuilder();

  /**
   * <pre>
   **
   * The latest transformation is in the back of the list.
   * </pre>
   *
   * <code>repeated .alluxio.grpc.table.Transformation transformations = 3;</code>
   */
  java.util.List<alluxio.grpc.table.Transformation> 
      getTransformationsList();
  /**
   * <pre>
   **
   * The latest transformation is in the back of the list.
   * </pre>
   *
   * <code>repeated .alluxio.grpc.table.Transformation transformations = 3;</code>
   */
  alluxio.grpc.table.Transformation getTransformations(int index);
  /**
   * <pre>
   **
   * The latest transformation is in the back of the list.
   * </pre>
   *
   * <code>repeated .alluxio.grpc.table.Transformation transformations = 3;</code>
   */
  int getTransformationsCount();
  /**
   * <pre>
   **
   * The latest transformation is in the back of the list.
   * </pre>
   *
   * <code>repeated .alluxio.grpc.table.Transformation transformations = 3;</code>
   */
  java.util.List<? extends alluxio.grpc.table.TransformationOrBuilder> 
      getTransformationsOrBuilderList();
  /**
   * <pre>
   **
   * The latest transformation is in the back of the list.
   * </pre>
   *
   * <code>repeated .alluxio.grpc.table.Transformation transformations = 3;</code>
   */
  alluxio.grpc.table.TransformationOrBuilder getTransformationsOrBuilder(
      int index);

  /**
   * <code>optional int64 version = 4;</code>
   * @return Whether the version field is set.
   */
  boolean hasVersion();
  /**
   * <code>optional int64 version = 4;</code>
   * @return The version.
   */
  long getVersion();

  /**
   * <code>optional int64 version_creation_time = 5;</code>
   * @return Whether the versionCreationTime field is set.
   */
  boolean hasVersionCreationTime();
  /**
   * <code>optional int64 version_creation_time = 5;</code>
   * @return The versionCreationTime.
   */
  long getVersionCreationTime();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface DeletePOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.DeletePOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional bool recursive = 1;</code>
   * @return Whether the recursive field is set.
   */
  boolean hasRecursive();
  /**
   * <code>optional bool recursive = 1;</code>
   * @return The recursive.
   */
  boolean getRecursive();

  /**
   * <code>optional bool alluxioOnly = 2;</code>
   * @return Whether the alluxioOnly field is set.
   */
  boolean hasAlluxioOnly();
  /**
   * <code>optional bool alluxioOnly = 2;</code>
   * @return The alluxioOnly.
   */
  boolean getAlluxioOnly();

  /**
   * <code>optional bool unchecked = 3;</code>
   * @return Whether the unchecked field is set.
   */
  boolean hasUnchecked();
  /**
   * <code>optional bool unchecked = 3;</code>
   * @return The unchecked.
   */
  boolean getUnchecked();

  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   * @return Whether the commonOptions field is set.
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   * @return The commonOptions.
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 4;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface ExistsPOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.ExistsPOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.file.LoadMetadataPType loadMetadataType = 1;</code>
   */
  boolean hasLoadMetadataType();
  /**
   * <code>optional .alluxio.grpc.file.LoadMetadataPType loadMetadataType = 1;</code>
   */
  alluxio.grpc.LoadMetadataPType getLoadMetadataType();

  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 2;</code>
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 2;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 2;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface UpdateUfsModePRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.UpdateUfsModePRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the ufs path 
   * </pre>
   *
   * <code>optional string ufsPath = 1;</code>
   */
  boolean hasUfsPath();
  /**
   * <pre>
   ** the ufs path 
   * </pre>
   *
   * <code>optional string ufsPath = 1;</code>
   */
  java.lang.String getUfsPath();
  /**
   * <pre>
   ** the ufs path 
   * </pre>
   *
   * <code>optional string ufsPath = 1;</code>
   */
  com.google.protobuf.ByteString
      getUfsPathBytes();

  /**
   * <code>optional .alluxio.grpc.file.UpdateUfsModePOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.UpdateUfsModePOptions options = 2;</code>
   */
  alluxio.grpc.UpdateUfsModePOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.UpdateUfsModePOptions options = 2;</code>
   */
  alluxio.grpc.UpdateUfsModePOptionsOrBuilder getOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: key_value_master.proto

package alluxio.grpc;

public interface MergeStorePRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.MergeStorePRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string fromPath = 1;</code>
   */
  boolean hasFromPath();
  /**
   * <code>optional string fromPath = 1;</code>
   */
  java.lang.String getFromPath();
  /**
   * <code>optional string fromPath = 1;</code>
   */
  com.google.protobuf.ByteString
      getFromPathBytes();

  /**
   * <code>optional string toPath = 2;</code>
   */
  boolean hasToPath();
  /**
   * <code>optional string toPath = 2;</code>
   */
  java.lang.String getToPath();
  /**
   * <code>optional string toPath = 2;</code>
   */
  com.google.protobuf.ByteString
      getToPathBytes();

  /**
   * <code>optional .alluxio.grpc.MergeStorePOptions options = 3;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.MergeStorePOptions options = 3;</code>
   */
  alluxio.grpc.MergeStorePOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.MergeStorePOptions options = 3;</code>
   */
  alluxio.grpc.MergeStorePOptionsOrBuilder getOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

public interface PModeOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.PMode)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>required .alluxio.grpc.Bits ownerBits = 1;</code>
   */
  boolean hasOwnerBits();
  /**
   * <code>required .alluxio.grpc.Bits ownerBits = 1;</code>
   */
  alluxio.grpc.Bits getOwnerBits();

  /**
   * <code>required .alluxio.grpc.Bits groupBits = 2;</code>
   */
  boolean hasGroupBits();
  /**
   * <code>required .alluxio.grpc.Bits groupBits = 2;</code>
   */
  alluxio.grpc.Bits getGroupBits();

  /**
   * <code>required .alluxio.grpc.Bits otherBits = 3;</code>
   */
  boolean hasOtherBits();
  /**
   * <code>required .alluxio.grpc.Bits otherBits = 3;</code>
   */
  alluxio.grpc.Bits getOtherBits();
}

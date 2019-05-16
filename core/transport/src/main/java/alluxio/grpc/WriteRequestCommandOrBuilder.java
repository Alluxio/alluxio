// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_worker.proto

package alluxio.grpc;

public interface WriteRequestCommandOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.WriteRequestCommand)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.block.RequestType type = 1;</code>
   */
  boolean hasType();
  /**
   * <code>optional .alluxio.grpc.block.RequestType type = 1;</code>
   */
  alluxio.grpc.RequestType getType();

  /**
   * <pre>
   * The block ID or UFS file ID.
   * </pre>
   *
   * <code>optional int64 id = 2;</code>
   */
  boolean hasId();
  /**
   * <pre>
   * The block ID or UFS file ID.
   * </pre>
   *
   * <code>optional int64 id = 2;</code>
   */
  long getId();

  /**
   * <code>optional int64 offset = 3;</code>
   */
  boolean hasOffset();
  /**
   * <code>optional int64 offset = 3;</code>
   */
  long getOffset();

  /**
   * <pre>
   * This is only applicable for block write.
   * </pre>
   *
   * <code>optional int32 tier = 4;</code>
   */
  boolean hasTier();
  /**
   * <pre>
   * This is only applicable for block write.
   * </pre>
   *
   * <code>optional int32 tier = 4;</code>
   */
  int getTier();

  /**
   * <code>optional bool flush = 5;</code>
   */
  boolean hasFlush();
  /**
   * <code>optional bool flush = 5;</code>
   */
  boolean getFlush();

  /**
   * <pre>
   * Cancel, close and error will be handled by standard gRPC stream APIs.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.CreateUfsFileOptions create_ufs_file_options = 6;</code>
   */
  boolean hasCreateUfsFileOptions();
  /**
   * <pre>
   * Cancel, close and error will be handled by standard gRPC stream APIs.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.CreateUfsFileOptions create_ufs_file_options = 6;</code>
   */
  alluxio.proto.dataserver.Protocol.CreateUfsFileOptions getCreateUfsFileOptions();
  /**
   * <pre>
   * Cancel, close and error will be handled by standard gRPC stream APIs.
   * </pre>
   *
   * <code>optional .alluxio.proto.dataserver.CreateUfsFileOptions create_ufs_file_options = 6;</code>
   */
  alluxio.proto.dataserver.Protocol.CreateUfsFileOptionsOrBuilder getCreateUfsFileOptionsOrBuilder();

  /**
   * <code>optional .alluxio.proto.dataserver.CreateUfsBlockOptions create_ufs_block_options = 7;</code>
   */
  boolean hasCreateUfsBlockOptions();
  /**
   * <code>optional .alluxio.proto.dataserver.CreateUfsBlockOptions create_ufs_block_options = 7;</code>
   */
  alluxio.proto.dataserver.Protocol.CreateUfsBlockOptions getCreateUfsBlockOptions();
  /**
   * <code>optional .alluxio.proto.dataserver.CreateUfsBlockOptions create_ufs_block_options = 7;</code>
   */
  alluxio.proto.dataserver.Protocol.CreateUfsBlockOptionsOrBuilder getCreateUfsBlockOptionsOrBuilder();

  /**
   * <code>optional string medium_type = 8;</code>
   */
  boolean hasMediumType();
  /**
   * <code>optional string medium_type = 8;</code>
   */
  java.lang.String getMediumType();
  /**
   * <code>optional string medium_type = 8;</code>
   */
  com.google.protobuf.ByteString
      getMediumTypeBytes();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface CommitBlockPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.CommitBlockPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   * @return Whether the workerId field is set.
   */
  boolean hasWorkerId();
  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   * @return The workerId.
   */
  long getWorkerId();

  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 usedBytesOnTier = 2;</code>
   * @return Whether the usedBytesOnTier field is set.
   */
  boolean hasUsedBytesOnTier();
  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 usedBytesOnTier = 2;</code>
   * @return The usedBytesOnTier.
   */
  long getUsedBytesOnTier();

  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   * @return Whether the tierAlias field is set.
   */
  boolean hasTierAlias();
  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   * @return The tierAlias.
   */
  java.lang.String getTierAlias();
  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   * @return The bytes for tierAlias.
   */
  com.google.protobuf.ByteString
      getTierAliasBytes();

  /**
   * <pre>
   ** the id of the block being committed 
   * </pre>
   *
   * <code>optional int64 blockId = 4;</code>
   * @return Whether the blockId field is set.
   */
  boolean hasBlockId();
  /**
   * <pre>
   ** the id of the block being committed 
   * </pre>
   *
   * <code>optional int64 blockId = 4;</code>
   * @return The blockId.
   */
  long getBlockId();

  /**
   * <pre>
   ** the length of the block being committed 
   * </pre>
   *
   * <code>optional int64 length = 5;</code>
   * @return Whether the length field is set.
   */
  boolean hasLength();
  /**
   * <pre>
   ** the length of the block being committed 
   * </pre>
   *
   * <code>optional int64 length = 5;</code>
   * @return The length.
   */
  long getLength();

  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   * @return Whether the options field is set.
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   * @return The options.
   */
  alluxio.grpc.CommitBlockPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   */
  alluxio.grpc.CommitBlockPOptionsOrBuilder getOptionsOrBuilder();

  /**
   * <code>optional string mediumType = 7;</code>
   * @return Whether the mediumType field is set.
   */
  boolean hasMediumType();
  /**
   * <code>optional string mediumType = 7;</code>
   * @return The mediumType.
   */
  java.lang.String getMediumType();
  /**
   * <code>optional string mediumType = 7;</code>
   * @return The bytes for mediumType.
   */
  com.google.protobuf.ByteString
      getMediumTypeBytes();
}

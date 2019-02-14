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
   */
  boolean hasWorkerId();
  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   */
  long getWorkerId();

  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 usedBytesOnTier = 2;</code>
   */
  boolean hasUsedBytesOnTier();
  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 usedBytesOnTier = 2;</code>
   */
  long getUsedBytesOnTier();

  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   */
  boolean hasTierAlias();
  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   */
  java.lang.String getTierAlias();
  /**
   * <pre>
   ** the alias of the target tier 
   * </pre>
   *
   * <code>optional string tierAlias = 3;</code>
   */
  com.google.protobuf.ByteString
      getTierAliasBytes();

  /**
   * <pre>
   ** the id of the block being committed 
   * </pre>
   *
   * <code>optional int64 blockId = 4;</code>
   */
  boolean hasBlockId();
  /**
   * <pre>
   ** the id of the block being committed 
   * </pre>
   *
   * <code>optional int64 blockId = 4;</code>
   */
  long getBlockId();

  /**
   * <pre>
   ** the length of the block being committed 
   * </pre>
   *
   * <code>optional int64 length = 5;</code>
   */
  boolean hasLength();
  /**
   * <pre>
   ** the length of the block being committed 
   * </pre>
   *
   * <code>optional int64 length = 5;</code>
   */
  long getLength();

  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   */
  alluxio.grpc.CommitBlockPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockPOptions options = 6;</code>
   */
  alluxio.grpc.CommitBlockPOptionsOrBuilder getOptionsOrBuilder();
}

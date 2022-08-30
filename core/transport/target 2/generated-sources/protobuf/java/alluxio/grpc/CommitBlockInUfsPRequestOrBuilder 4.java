// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface CommitBlockInUfsPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.CommitBlockInUfsPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 blockId = 1;</code>
   * @return Whether the blockId field is set.
   */
  boolean hasBlockId();
  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 blockId = 1;</code>
   * @return The blockId.
   */
  long getBlockId();

  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 length = 2;</code>
   * @return Whether the length field is set.
   */
  boolean hasLength();
  /**
   * <pre>
   ** the space used in bytes on the target tier 
   * </pre>
   *
   * <code>optional int64 length = 2;</code>
   * @return The length.
   */
  long getLength();

  /**
   * <code>optional .alluxio.grpc.block.CommitBlockInUfsPOptions options = 3;</code>
   * @return Whether the options field is set.
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockInUfsPOptions options = 3;</code>
   * @return The options.
   */
  alluxio.grpc.CommitBlockInUfsPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.block.CommitBlockInUfsPOptions options = 3;</code>
   */
  alluxio.grpc.CommitBlockInUfsPOptionsOrBuilder getOptionsOrBuilder();
}

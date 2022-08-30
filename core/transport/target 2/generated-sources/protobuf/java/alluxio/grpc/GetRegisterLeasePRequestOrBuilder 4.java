// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface GetRegisterLeasePRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.GetRegisterLeasePRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 workerId = 1;</code>
   * @return Whether the workerId field is set.
   */
  boolean hasWorkerId();
  /**
   * <code>optional int64 workerId = 1;</code>
   * @return The workerId.
   */
  long getWorkerId();

  /**
   * <pre>
   ** This may not accurate because the worker can add/delete blocks before sending the RegisterWorkerPRequest 
   * </pre>
   *
   * <code>optional int64 blockCount = 2;</code>
   * @return Whether the blockCount field is set.
   */
  boolean hasBlockCount();
  /**
   * <pre>
   ** This may not accurate because the worker can add/delete blocks before sending the RegisterWorkerPRequest 
   * </pre>
   *
   * <code>optional int64 blockCount = 2;</code>
   * @return The blockCount.
   */
  long getBlockCount();
}

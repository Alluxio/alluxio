// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface GetBlockInfoPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.GetBlockInfoPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.BlockInfo blockInfo = 1;</code>
   * @return Whether the blockInfo field is set.
   */
  boolean hasBlockInfo();
  /**
   * <code>optional .alluxio.grpc.BlockInfo blockInfo = 1;</code>
   * @return The blockInfo.
   */
  alluxio.grpc.BlockInfo getBlockInfo();
  /**
   * <code>optional .alluxio.grpc.BlockInfo blockInfo = 1;</code>
   */
  alluxio.grpc.BlockInfoOrBuilder getBlockInfoOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface MasterHeartbeatPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.MasterHeartbeatPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.meta.MetaCommand command = 1;</code>
   * @return Whether the command field is set.
   */
  boolean hasCommand();
  /**
   * <code>optional .alluxio.grpc.meta.MetaCommand command = 1;</code>
   * @return The command.
   */
  alluxio.grpc.MetaCommand getCommand();
}

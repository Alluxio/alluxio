// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface GetUfsInfoPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.GetUfsInfoPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.file.UfsInfo ufsInfo = 1;</code>
   * @return Whether the ufsInfo field is set.
   */
  boolean hasUfsInfo();
  /**
   * <code>optional .alluxio.grpc.file.UfsInfo ufsInfo = 1;</code>
   * @return The ufsInfo.
   */
  alluxio.grpc.UfsInfo getUfsInfo();
  /**
   * <code>optional .alluxio.grpc.file.UfsInfo ufsInfo = 1;</code>
   */
  alluxio.grpc.UfsInfoOrBuilder getUfsInfoOrBuilder();
}

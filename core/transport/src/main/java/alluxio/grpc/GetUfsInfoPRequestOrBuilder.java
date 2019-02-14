// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface GetUfsInfoPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.GetUfsInfoPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the id of the ufs 
   * </pre>
   *
   * <code>optional int64 mountId = 1;</code>
   */
  boolean hasMountId();
  /**
   * <pre>
   ** the id of the ufs 
   * </pre>
   *
   * <code>optional int64 mountId = 1;</code>
   */
  long getMountId();

  /**
   * <code>optional .alluxio.grpc.file.GetUfsInfoPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.GetUfsInfoPOptions options = 2;</code>
   */
  alluxio.grpc.GetUfsInfoPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.GetUfsInfoPOptions options = 2;</code>
   */
  alluxio.grpc.GetUfsInfoPOptionsOrBuilder getOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

public interface WorkerNetAddressOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.WorkerNetAddress)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string host = 1;</code>
   */
  boolean hasHost();
  /**
   * <code>optional string host = 1;</code>
   */
  java.lang.String getHost();
  /**
   * <code>optional string host = 1;</code>
   */
  com.google.protobuf.ByteString
      getHostBytes();

  /**
   * <code>optional int32 rpcPort = 2;</code>
   */
  boolean hasRpcPort();
  /**
   * <code>optional int32 rpcPort = 2;</code>
   */
  int getRpcPort();

  /**
   * <code>optional int32 dataPort = 3;</code>
   */
  boolean hasDataPort();
  /**
   * <code>optional int32 dataPort = 3;</code>
   */
  int getDataPort();

  /**
   * <code>optional int32 webPort = 4;</code>
   */
  boolean hasWebPort();
  /**
   * <code>optional int32 webPort = 4;</code>
   */
  int getWebPort();

  /**
   * <code>optional string domainSocketPath = 5;</code>
   */
  boolean hasDomainSocketPath();
  /**
   * <code>optional string domainSocketPath = 5;</code>
   */
  java.lang.String getDomainSocketPath();
  /**
   * <code>optional string domainSocketPath = 5;</code>
   */
  com.google.protobuf.ByteString
      getDomainSocketPathBytes();

  /**
   * <code>optional .alluxio.grpc.TieredIdentity tieredIdentity = 6;</code>
   */
  boolean hasTieredIdentity();
  /**
   * <code>optional .alluxio.grpc.TieredIdentity tieredIdentity = 6;</code>
   */
  alluxio.grpc.TieredIdentity getTieredIdentity();
  /**
   * <code>optional .alluxio.grpc.TieredIdentity tieredIdentity = 6;</code>
   */
  alluxio.grpc.TieredIdentityOrBuilder getTieredIdentityOrBuilder();

  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional int32 secureRpcPort = 1001;</code>
   */
  boolean hasSecureRpcPort();
  /**
   * <pre>
   * ALLUXIO CS ADD
   * </pre>
   *
   * <code>optional int32 secureRpcPort = 1001;</code>
   */
  int getSecureRpcPort();
}

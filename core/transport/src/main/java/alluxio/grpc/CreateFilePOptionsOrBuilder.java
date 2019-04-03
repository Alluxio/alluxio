// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface CreateFilePOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.CreateFilePOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 blockSizeBytes = 1;</code>
   */
  boolean hasBlockSizeBytes();
  /**
   * <code>optional int64 blockSizeBytes = 1;</code>
   */
  long getBlockSizeBytes();

  /**
   * <code>optional bool recursive = 2;</code>
   */
  boolean hasRecursive();
  /**
   * <code>optional bool recursive = 2;</code>
   */
  boolean getRecursive();

  /**
   * <code>optional .alluxio.grpc.PMode mode = 3;</code>
   */
  boolean hasMode();
  /**
   * <code>optional .alluxio.grpc.PMode mode = 3;</code>
   */
  alluxio.grpc.PMode getMode();
  /**
   * <code>optional .alluxio.grpc.PMode mode = 3;</code>
   */
  alluxio.grpc.PModeOrBuilder getModeOrBuilder();

  /**
   * <code>optional int32 replicationMax = 4;</code>
   */
  boolean hasReplicationMax();
  /**
   * <code>optional int32 replicationMax = 4;</code>
   */
  int getReplicationMax();

  /**
   * <code>optional int32 replicationMin = 5;</code>
   */
  boolean hasReplicationMin();
  /**
   * <code>optional int32 replicationMin = 5;</code>
   */
  int getReplicationMin();

  /**
   * <code>optional int32 replicationDurable = 6;</code>
   */
  boolean hasReplicationDurable();
  /**
   * <code>optional int32 replicationDurable = 6;</code>
   */
  int getReplicationDurable();

  /**
   * <code>optional int32 writeTier = 7;</code>
   */
  boolean hasWriteTier();
  /**
   * <code>optional int32 writeTier = 7;</code>
   */
  int getWriteTier();

  /**
   * <code>optional .alluxio.grpc.file.WritePType writeType = 8;</code>
   */
  boolean hasWriteType();
  /**
   * <code>optional .alluxio.grpc.file.WritePType writeType = 8;</code>
   */
  alluxio.grpc.WritePType getWriteType();

  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 9;</code>
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 9;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 9;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}

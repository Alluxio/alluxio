// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: file_system_master_options.proto

package alluxio.grpc;

public interface CreateFilePOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.CreateFilePOptions)
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
   * <code>optional bool persisted = 2;</code>
   */
  boolean hasPersisted();
  /**
   * <code>optional bool persisted = 2;</code>
   */
  boolean getPersisted();

  /**
   * <code>optional bool recursive = 3;</code>
   */
  boolean hasRecursive();
  /**
   * <code>optional bool recursive = 3;</code>
   */
  boolean getRecursive();

  /**
   * <code>optional int32 mode = 4;</code>
   */
  boolean hasMode();
  /**
   * <code>optional int32 mode = 4;</code>
   */
  int getMode();

  /**
   * <code>optional int32 replicationMax = 5;</code>
   */
  boolean hasReplicationMax();
  /**
   * <code>optional int32 replicationMax = 5;</code>
   */
  int getReplicationMax();

  /**
   * <code>optional int32 replicationMin = 6;</code>
   */
  boolean hasReplicationMin();
  /**
   * <code>optional int32 replicationMin = 6;</code>
   */
  int getReplicationMin();

  /**
   * <code>optional int32 replicationDurable = 7;</code>
   */
  boolean hasReplicationDurable();
  /**
   * <code>optional int32 replicationDurable = 7;</code>
   */
  int getReplicationDurable();

  /**
   * <code>optional string fileWriteLocationPolicy = 8;</code>
   */
  boolean hasFileWriteLocationPolicy();
  /**
   * <code>optional string fileWriteLocationPolicy = 8;</code>
   */
  java.lang.String getFileWriteLocationPolicy();
  /**
   * <code>optional string fileWriteLocationPolicy = 8;</code>
   */
  com.google.protobuf.ByteString
      getFileWriteLocationPolicyBytes();

  /**
   * <code>optional int32 writeTier = 9;</code>
   */
  boolean hasWriteTier();
  /**
   * <code>optional int32 writeTier = 9;</code>
   */
  int getWriteTier();

  /**
   * <code>optional .alluxio.grpc.WritePType writeType = 10;</code>
   */
  boolean hasWriteType();
  /**
   * <code>optional .alluxio.grpc.WritePType writeType = 10;</code>
   */
  alluxio.grpc.WritePType getWriteType();

  /**
   * <code>optional .alluxio.grpc.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  boolean hasCommonOptions();
  /**
   * <code>optional .alluxio.grpc.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions();
  /**
   * <code>optional .alluxio.grpc.FileSystemMasterCommonPOptions commonOptions = 11;</code>
   */
  alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder();
}

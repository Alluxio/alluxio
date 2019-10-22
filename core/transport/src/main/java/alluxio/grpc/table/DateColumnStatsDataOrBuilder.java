// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface DateColumnStatsDataOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.DateColumnStatsData)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.table.Date low_value = 1;</code>
   */
  boolean hasLowValue();
  /**
   * <code>optional .alluxio.grpc.table.Date low_value = 1;</code>
   */
  alluxio.grpc.table.Date getLowValue();
  /**
   * <code>optional .alluxio.grpc.table.Date low_value = 1;</code>
   */
  alluxio.grpc.table.DateOrBuilder getLowValueOrBuilder();

  /**
   * <code>optional .alluxio.grpc.table.Date high_value = 2;</code>
   */
  boolean hasHighValue();
  /**
   * <code>optional .alluxio.grpc.table.Date high_value = 2;</code>
   */
  alluxio.grpc.table.Date getHighValue();
  /**
   * <code>optional .alluxio.grpc.table.Date high_value = 2;</code>
   */
  alluxio.grpc.table.DateOrBuilder getHighValueOrBuilder();

  /**
   * <code>optional int64 num_nulls = 3;</code>
   */
  boolean hasNumNulls();
  /**
   * <code>optional int64 num_nulls = 3;</code>
   */
  long getNumNulls();

  /**
   * <code>optional int64 num_distincts = 4;</code>
   */
  boolean hasNumDistincts();
  /**
   * <code>optional int64 num_distincts = 4;</code>
   */
  long getNumDistincts();

  /**
   * <code>optional string bit_vectors = 5;</code>
   */
  boolean hasBitVectors();
  /**
   * <code>optional string bit_vectors = 5;</code>
   */
  java.lang.String getBitVectors();
  /**
   * <code>optional string bit_vectors = 5;</code>
   */
  com.google.protobuf.ByteString
      getBitVectorsBytes();
}

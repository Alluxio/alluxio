// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface ColumnStatisticsInfoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.ColumnStatisticsInfo)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string col_name = 1;</code>
   * @return Whether the colName field is set.
   */
  boolean hasColName();
  /**
   * <code>optional string col_name = 1;</code>
   * @return The colName.
   */
  java.lang.String getColName();
  /**
   * <code>optional string col_name = 1;</code>
   * @return The bytes for colName.
   */
  com.google.protobuf.ByteString
      getColNameBytes();

  /**
   * <code>optional string col_type = 2;</code>
   * @return Whether the colType field is set.
   */
  boolean hasColType();
  /**
   * <code>optional string col_type = 2;</code>
   * @return The colType.
   */
  java.lang.String getColType();
  /**
   * <code>optional string col_type = 2;</code>
   * @return The bytes for colType.
   */
  com.google.protobuf.ByteString
      getColTypeBytes();

  /**
   * <code>optional .alluxio.grpc.table.ColumnStatisticsData data = 3;</code>
   * @return Whether the data field is set.
   */
  boolean hasData();
  /**
   * <code>optional .alluxio.grpc.table.ColumnStatisticsData data = 3;</code>
   * @return The data.
   */
  alluxio.grpc.table.ColumnStatisticsData getData();
  /**
   * <code>optional .alluxio.grpc.table.ColumnStatisticsData data = 3;</code>
   */
  alluxio.grpc.table.ColumnStatisticsDataOrBuilder getDataOrBuilder();
}

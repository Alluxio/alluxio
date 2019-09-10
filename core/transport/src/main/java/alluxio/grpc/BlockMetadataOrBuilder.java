// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog_master.proto

package alluxio.grpc;

public interface BlockMetadataOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.BlockMetadata)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 row_count = 1;</code>
   */
  boolean hasRowCount();
  /**
   * <code>optional int64 row_count = 1;</code>
   */
  long getRowCount();

  /**
   * <code>optional int64 total_byte_count = 2;</code>
   */
  boolean hasTotalByteCount();
  /**
   * <code>optional int64 total_byte_count = 2;</code>
   */
  long getTotalByteCount();

  /**
   * <code>optional string path = 3;</code>
   */
  boolean hasPath();
  /**
   * <code>optional string path = 3;</code>
   */
  java.lang.String getPath();
  /**
   * <code>optional string path = 3;</code>
   */
  com.google.protobuf.ByteString
      getPathBytes();

  /**
   * <code>repeated .alluxio.grpc.ColumnChunkMetaData col_data = 4;</code>
   */
  java.util.List<alluxio.grpc.ColumnChunkMetaData> 
      getColDataList();
  /**
   * <code>repeated .alluxio.grpc.ColumnChunkMetaData col_data = 4;</code>
   */
  alluxio.grpc.ColumnChunkMetaData getColData(int index);
  /**
   * <code>repeated .alluxio.grpc.ColumnChunkMetaData col_data = 4;</code>
   */
  int getColDataCount();
  /**
   * <code>repeated .alluxio.grpc.ColumnChunkMetaData col_data = 4;</code>
   */
  java.util.List<? extends alluxio.grpc.ColumnChunkMetaDataOrBuilder> 
      getColDataOrBuilderList();
  /**
   * <code>repeated .alluxio.grpc.ColumnChunkMetaData col_data = 4;</code>
   */
  alluxio.grpc.ColumnChunkMetaDataOrBuilder getColDataOrBuilder(
      int index);
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface GetTablePResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.GetTablePResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.table.TableInfo table_info = 1;</code>
   * @return Whether the tableInfo field is set.
   */
  boolean hasTableInfo();
  /**
   * <code>optional .alluxio.grpc.table.TableInfo table_info = 1;</code>
   * @return The tableInfo.
   */
  alluxio.grpc.table.TableInfo getTableInfo();
  /**
   * <code>optional .alluxio.grpc.table.TableInfo table_info = 1;</code>
   */
  alluxio.grpc.table.TableInfoOrBuilder getTableInfoOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog_master.proto

package alluxio.grpc;

public interface GetAllDatabasesPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.GetAllDatabasesPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated string database = 1;</code>
   */
  java.util.List<java.lang.String>
      getDatabaseList();
  /**
   * <code>repeated string database = 1;</code>
   */
  int getDatabaseCount();
  /**
   * <code>repeated string database = 1;</code>
   */
  java.lang.String getDatabase(int index);
  /**
   * <code>repeated string database = 1;</code>
   */
  com.google.protobuf.ByteString
      getDatabaseBytes(int index);
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface FieldSchemaOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.FieldSchema)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional uint32 id = 1;</code>
   */
  boolean hasId();
  /**
   * <code>optional uint32 id = 1;</code>
   */
  int getId();

  /**
   * <code>optional string name = 2;</code>
   */
  boolean hasName();
  /**
   * <code>optional string name = 2;</code>
   */
  java.lang.String getName();
  /**
   * <code>optional string name = 2;</code>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>optional string type = 3;</code>
   */
  boolean hasType();
  /**
   * <code>optional string type = 3;</code>
   */
  java.lang.String getType();
  /**
   * <code>optional string type = 3;</code>
   */
  com.google.protobuf.ByteString
      getTypeBytes();

  /**
   * <code>optional string comment = 4;</code>
   */
  boolean hasComment();
  /**
   * <code>optional string comment = 4;</code>
   */
  java.lang.String getComment();
  /**
   * <code>optional string comment = 4;</code>
   */
  com.google.protobuf.ByteString
      getCommentBytes();
}

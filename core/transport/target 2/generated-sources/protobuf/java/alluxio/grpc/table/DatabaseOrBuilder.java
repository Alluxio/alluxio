// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

public interface DatabaseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.Database)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string db_name = 1;</code>
   * @return Whether the dbName field is set.
   */
  boolean hasDbName();
  /**
   * <code>optional string db_name = 1;</code>
   * @return The dbName.
   */
  java.lang.String getDbName();
  /**
   * <code>optional string db_name = 1;</code>
   * @return The bytes for dbName.
   */
  com.google.protobuf.ByteString
      getDbNameBytes();

  /**
   * <code>optional string description = 2;</code>
   * @return Whether the description field is set.
   */
  boolean hasDescription();
  /**
   * <code>optional string description = 2;</code>
   * @return The description.
   */
  java.lang.String getDescription();
  /**
   * <code>optional string description = 2;</code>
   * @return The bytes for description.
   */
  com.google.protobuf.ByteString
      getDescriptionBytes();

  /**
   * <code>optional string location = 3;</code>
   * @return Whether the location field is set.
   */
  boolean hasLocation();
  /**
   * <code>optional string location = 3;</code>
   * @return The location.
   */
  java.lang.String getLocation();
  /**
   * <code>optional string location = 3;</code>
   * @return The bytes for location.
   */
  com.google.protobuf.ByteString
      getLocationBytes();

  /**
   * <code>map&lt;string, string&gt; parameter = 4;</code>
   */
  int getParameterCount();
  /**
   * <code>map&lt;string, string&gt; parameter = 4;</code>
   */
  boolean containsParameter(
      java.lang.String key);
  /**
   * Use {@link #getParameterMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getParameter();
  /**
   * <code>map&lt;string, string&gt; parameter = 4;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getParameterMap();
  /**
   * <code>map&lt;string, string&gt; parameter = 4;</code>
   */

  java.lang.String getParameterOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; parameter = 4;</code>
   */

  java.lang.String getParameterOrThrow(
      java.lang.String key);

  /**
   * <code>optional string owner_name = 5;</code>
   * @return Whether the ownerName field is set.
   */
  boolean hasOwnerName();
  /**
   * <code>optional string owner_name = 5;</code>
   * @return The ownerName.
   */
  java.lang.String getOwnerName();
  /**
   * <code>optional string owner_name = 5;</code>
   * @return The bytes for ownerName.
   */
  com.google.protobuf.ByteString
      getOwnerNameBytes();

  /**
   * <code>optional .alluxio.grpc.table.PrincipalType owner_type = 6;</code>
   * @return Whether the ownerType field is set.
   */
  boolean hasOwnerType();
  /**
   * <code>optional .alluxio.grpc.table.PrincipalType owner_type = 6;</code>
   * @return The ownerType.
   */
  alluxio.grpc.table.PrincipalType getOwnerType();

  /**
   * <code>optional string comment = 7;</code>
   * @return Whether the comment field is set.
   */
  boolean hasComment();
  /**
   * <code>optional string comment = 7;</code>
   * @return The comment.
   */
  java.lang.String getComment();
  /**
   * <code>optional string comment = 7;</code>
   * @return The bytes for comment.
   */
  com.google.protobuf.ByteString
      getCommentBytes();
}

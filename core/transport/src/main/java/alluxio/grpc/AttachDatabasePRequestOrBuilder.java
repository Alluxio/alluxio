// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/catalog_master.proto

package alluxio.grpc;

public interface AttachDatabasePRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.AttachDatabasePRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string db_name = 1;</code>
   */
  boolean hasDbName();
  /**
   * <code>optional string db_name = 1;</code>
   */
  java.lang.String getDbName();
  /**
   * <code>optional string db_name = 1;</code>
   */
  com.google.protobuf.ByteString
      getDbNameBytes();

  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  int getOptionsCount();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  boolean containsOptions(
      java.lang.String key);
  /**
   * Use {@link #getOptionsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getOptions();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getOptionsMap();
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */

  java.lang.String getOptionsOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; options = 2;</code>
   */

  java.lang.String getOptionsOrThrow(
      java.lang.String key);
}

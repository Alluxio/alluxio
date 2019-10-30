// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/layout/hive/hive.proto

package alluxio.grpc.table.layout.hive;

public interface StorageFormatOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.table.layout.StorageFormat)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string serde = 1;</code>
   */
  boolean hasSerde();
  /**
   * <code>optional string serde = 1;</code>
   */
  java.lang.String getSerde();
  /**
   * <code>optional string serde = 1;</code>
   */
  com.google.protobuf.ByteString
      getSerdeBytes();

  /**
   * <code>optional string input_format = 2;</code>
   */
  boolean hasInputFormat();
  /**
   * <code>optional string input_format = 2;</code>
   */
  java.lang.String getInputFormat();
  /**
   * <code>optional string input_format = 2;</code>
   */
  com.google.protobuf.ByteString
      getInputFormatBytes();

  /**
   * <code>optional string output_format = 3;</code>
   */
  boolean hasOutputFormat();
  /**
   * <code>optional string output_format = 3;</code>
   */
  java.lang.String getOutputFormat();
  /**
   * <code>optional string output_format = 3;</code>
   */
  com.google.protobuf.ByteString
      getOutputFormatBytes();

  /**
   * <code>map&lt;string, string&gt; serdelib_parameters = 4;</code>
   */
  int getSerdelibParametersCount();
  /**
   * <code>map&lt;string, string&gt; serdelib_parameters = 4;</code>
   */
  boolean containsSerdelibParameters(
      java.lang.String key);
  /**
   * Use {@link #getSerdelibParametersMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getSerdelibParameters();
  /**
   * <code>map&lt;string, string&gt; serdelib_parameters = 4;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getSerdelibParametersMap();
  /**
   * <code>map&lt;string, string&gt; serdelib_parameters = 4;</code>
   */

  java.lang.String getSerdelibParametersOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; serdelib_parameters = 4;</code>
   */

  java.lang.String getSerdelibParametersOrThrow(
      java.lang.String key);
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface UpdateConfigurationPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.UpdateConfigurationPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>map&lt;string, string&gt; properties = 1;</code>
   */
  int getPropertiesCount();
  /**
   * <code>map&lt;string, string&gt; properties = 1;</code>
   */
  boolean containsProperties(
      java.lang.String key);
  /**
   * Use {@link #getPropertiesMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getProperties();
  /**
   * <code>map&lt;string, string&gt; properties = 1;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getPropertiesMap();
  /**
   * <code>map&lt;string, string&gt; properties = 1;</code>
   */

  java.lang.String getPropertiesOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; properties = 1;</code>
   */

  java.lang.String getPropertiesOrThrow(
      java.lang.String key);
}

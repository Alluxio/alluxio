// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

public interface MetricOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.Metric)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string instance = 1;</code>
   * @return Whether the instance field is set.
   */
  boolean hasInstance();
  /**
   * <code>optional string instance = 1;</code>
   * @return The instance.
   */
  java.lang.String getInstance();
  /**
   * <code>optional string instance = 1;</code>
   * @return The bytes for instance.
   */
  com.google.protobuf.ByteString
      getInstanceBytes();

  /**
<<<<<<< HEAD
   * <code>optional string source = 2;</code>
   * @return Whether the source field is set.
=======
   * <code>optional string hostname = 2;</code>
   * @return Whether the hostname field is set.
>>>>>>> upstream/master
   */
  boolean hasSource();
  /**
<<<<<<< HEAD
   * <code>optional string source = 2;</code>
   * @return The source.
=======
   * <code>optional string hostname = 2;</code>
   * @return The hostname.
>>>>>>> upstream/master
   */
  java.lang.String getSource();
  /**
<<<<<<< HEAD
   * <code>optional string source = 2;</code>
   * @return The bytes for source.
=======
   * <code>optional string hostname = 2;</code>
   * @return The bytes for hostname.
>>>>>>> upstream/master
   */
  com.google.protobuf.ByteString
      getSourceBytes();

  /**
<<<<<<< HEAD
   * <code>optional string name = 3;</code>
=======
   * <code>optional string instanceId = 3;</code>
   * @return Whether the instanceId field is set.
   */
  boolean hasInstanceId();
  /**
   * <code>optional string instanceId = 3;</code>
   * @return The instanceId.
   */
  java.lang.String getInstanceId();
  /**
   * <code>optional string instanceId = 3;</code>
   * @return The bytes for instanceId.
   */
  com.google.protobuf.ByteString
      getInstanceIdBytes();

  /**
   * <code>optional string name = 4;</code>
>>>>>>> upstream/master
   * @return Whether the name field is set.
   */
  boolean hasName();
  /**
<<<<<<< HEAD
   * <code>optional string name = 3;</code>
=======
   * <code>optional string name = 4;</code>
>>>>>>> upstream/master
   * @return The name.
   */
  java.lang.String getName();
  /**
<<<<<<< HEAD
   * <code>optional string name = 3;</code>
=======
   * <code>optional string name = 4;</code>
>>>>>>> upstream/master
   * @return The bytes for name.
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
<<<<<<< HEAD
   * <code>optional double value = 4;</code>
=======
   * <code>optional double value = 5;</code>
>>>>>>> upstream/master
   * @return Whether the value field is set.
   */
  boolean hasValue();
  /**
<<<<<<< HEAD
   * <code>optional double value = 4;</code>
=======
   * <code>optional double value = 5;</code>
>>>>>>> upstream/master
   * @return The value.
   */
  double getValue();

  /**
<<<<<<< HEAD
   * <code>required .alluxio.grpc.MetricType metricType = 5;</code>
=======
   * <code>required .alluxio.grpc.MetricType metricType = 6;</code>
>>>>>>> upstream/master
   * @return Whether the metricType field is set.
   */
  boolean hasMetricType();
  /**
<<<<<<< HEAD
   * <code>required .alluxio.grpc.MetricType metricType = 5;</code>
=======
   * <code>required .alluxio.grpc.MetricType metricType = 6;</code>
>>>>>>> upstream/master
   * @return The metricType.
   */
  alluxio.grpc.MetricType getMetricType();

  /**
   * <code>map&lt;string, string&gt; tags = 6;</code>
   */
  int getTagsCount();
  /**
   * <code>map&lt;string, string&gt; tags = 6;</code>
   */
  boolean containsTags(
      java.lang.String key);
  /**
   * Use {@link #getTagsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.String>
  getTags();
  /**
   * <code>map&lt;string, string&gt; tags = 6;</code>
   */
  java.util.Map<java.lang.String, java.lang.String>
  getTagsMap();
  /**
   * <code>map&lt;string, string&gt; tags = 6;</code>
   */

  java.lang.String getTagsOrDefault(
      java.lang.String key,
      java.lang.String defaultValue);
  /**
   * <code>map&lt;string, string&gt; tags = 6;</code>
   */

  java.lang.String getTagsOrThrow(
      java.lang.String key);
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface GetMetricsPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.GetMetricsPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>map&lt;string, .alluxio.grpc.meta.MetricValue&gt; metrics = 1;</code>
   */
  int getMetricsCount();
  /**
   * <code>map&lt;string, .alluxio.grpc.meta.MetricValue&gt; metrics = 1;</code>
   */
  boolean containsMetrics(
      java.lang.String key);
  /**
   * Use {@link #getMetricsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, alluxio.grpc.MetricValue>
  getMetrics();
  /**
   * <code>map&lt;string, .alluxio.grpc.meta.MetricValue&gt; metrics = 1;</code>
   */
  java.util.Map<java.lang.String, alluxio.grpc.MetricValue>
  getMetricsMap();
  /**
   * <code>map&lt;string, .alluxio.grpc.meta.MetricValue&gt; metrics = 1;</code>
   */

  alluxio.grpc.MetricValue getMetricsOrDefault(
      java.lang.String key,
      alluxio.grpc.MetricValue defaultValue);
  /**
   * <code>map&lt;string, .alluxio.grpc.meta.MetricValue&gt; metrics = 1;</code>
   */

  alluxio.grpc.MetricValue getMetricsOrThrow(
      java.lang.String key);
}

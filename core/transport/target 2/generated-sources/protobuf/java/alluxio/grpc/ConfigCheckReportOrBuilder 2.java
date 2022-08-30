// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface ConfigCheckReportOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.ConfigCheckReport)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; errors = 1;</code>
   */
  int getErrorsCount();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; errors = 1;</code>
   */
  boolean containsErrors(
      java.lang.String key);
  /**
   * Use {@link #getErrorsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, alluxio.grpc.InconsistentProperties>
  getErrors();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; errors = 1;</code>
   */
  java.util.Map<java.lang.String, alluxio.grpc.InconsistentProperties>
  getErrorsMap();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; errors = 1;</code>
   */

  alluxio.grpc.InconsistentProperties getErrorsOrDefault(
      java.lang.String key,
      alluxio.grpc.InconsistentProperties defaultValue);
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; errors = 1;</code>
   */

  alluxio.grpc.InconsistentProperties getErrorsOrThrow(
      java.lang.String key);

  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; warns = 2;</code>
   */
  int getWarnsCount();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; warns = 2;</code>
   */
  boolean containsWarns(
      java.lang.String key);
  /**
   * Use {@link #getWarnsMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, alluxio.grpc.InconsistentProperties>
  getWarns();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; warns = 2;</code>
   */
  java.util.Map<java.lang.String, alluxio.grpc.InconsistentProperties>
  getWarnsMap();
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; warns = 2;</code>
   */

  alluxio.grpc.InconsistentProperties getWarnsOrDefault(
      java.lang.String key,
      alluxio.grpc.InconsistentProperties defaultValue);
  /**
   * <pre>
   * Scope name as key
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.meta.InconsistentProperties&gt; warns = 2;</code>
   */

  alluxio.grpc.InconsistentProperties getWarnsOrThrow(
      java.lang.String key);

  /**
   * <code>optional .alluxio.grpc.meta.ConfigStatus status = 3;</code>
   * @return Whether the status field is set.
   */
  boolean hasStatus();
  /**
   * <code>optional .alluxio.grpc.meta.ConfigStatus status = 3;</code>
   * @return The status.
   */
  alluxio.grpc.ConfigStatus getStatus();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

public interface FileSystemHeartbeatPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.file.FileSystemHeartbeatPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   */
  boolean hasWorkerId();
  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   */
  long getWorkerId();

  /**
   * <pre>
   ** the list of persisted files 
   * </pre>
   *
   * <code>repeated int64 persistedFiles = 2;</code>
   */
  java.util.List<java.lang.Long> getPersistedFilesList();
  /**
   * <pre>
   ** the list of persisted files 
   * </pre>
   *
   * <code>repeated int64 persistedFiles = 2;</code>
   */
  int getPersistedFilesCount();
  /**
   * <pre>
   ** the list of persisted files 
   * </pre>
   *
   * <code>repeated int64 persistedFiles = 2;</code>
   */
  long getPersistedFiles(int index);

  /**
   * <code>optional .alluxio.grpc.file.FileSystemHeartbeatPOptions options = 3;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemHeartbeatPOptions options = 3;</code>
   */
  alluxio.grpc.FileSystemHeartbeatPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.file.FileSystemHeartbeatPOptions options = 3;</code>
   */
  alluxio.grpc.FileSystemHeartbeatPOptionsOrBuilder getOptionsOrBuilder();
}

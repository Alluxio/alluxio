// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface CancelTaskCommandOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.CancelTaskCommand)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 jobId = 1;</code>
   */
  boolean hasJobId();
  /**
   * <code>optional int64 jobId = 1;</code>
   */
  long getJobId();

  /**
   * <code>optional int64 taskId = 2;</code>
   */
  boolean hasTaskId();
  /**
   * <code>optional int64 taskId = 2;</code>
   */
  long getTaskId();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface GetJobStatusPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.GetJobStatusPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 jobId = 1;</code>
   * @return Whether the jobId field is set.
   */
  boolean hasJobId();
  /**
   * <code>optional int64 jobId = 1;</code>
   * @return The jobId.
   */
  long getJobId();

  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   * @return Whether the options field is set.
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   * @return The options.
   */
  alluxio.grpc.GetJobStatusPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   */
  alluxio.grpc.GetJobStatusPOptionsOrBuilder getOptionsOrBuilder();
}

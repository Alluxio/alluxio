// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface GetJobStatusPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.GetJobStatusPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 id = 1;</code>
   */
  boolean hasId();
  /**
   * <code>optional int64 id = 1;</code>
   */
  long getId();

  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   */
  alluxio.grpc.GetJobStatusPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.job.GetJobStatusPOptions options = 2;</code>
   */
  alluxio.grpc.GetJobStatusPOptionsOrBuilder getOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface RunPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.RunPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional bytes jobConfig = 1;</code>
   */
  boolean hasJobConfig();
  /**
   * <code>optional bytes jobConfig = 1;</code>
   */
  com.google.protobuf.ByteString getJobConfig();

  /**
   * <code>optional .alluxio.grpc.job.RunPOptions options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.job.RunPOptions options = 2;</code>
   */
  alluxio.grpc.RunPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.job.RunPOptions options = 2;</code>
   */
  alluxio.grpc.RunPOptionsOrBuilder getOptionsOrBuilder();
}

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface JobHeartbeatPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.JobHeartbeatPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .alluxio.grpc.job.JobCommand commands = 1;</code>
   */
  java.util.List<alluxio.grpc.JobCommand> 
      getCommandsList();
  /**
   * <code>repeated .alluxio.grpc.job.JobCommand commands = 1;</code>
   */
  alluxio.grpc.JobCommand getCommands(int index);
  /**
   * <code>repeated .alluxio.grpc.job.JobCommand commands = 1;</code>
   */
  int getCommandsCount();
  /**
   * <code>repeated .alluxio.grpc.job.JobCommand commands = 1;</code>
   */
  java.util.List<? extends alluxio.grpc.JobCommandOrBuilder> 
      getCommandsOrBuilderList();
  /**
   * <code>repeated .alluxio.grpc.job.JobCommand commands = 1;</code>
   */
  alluxio.grpc.JobCommandOrBuilder getCommandsOrBuilder(
      int index);
}

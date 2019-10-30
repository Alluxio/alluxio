// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface JobInfoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.JobInfo)
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
   * <code>optional string errorMessage = 2;</code>
   */
  boolean hasErrorMessage();
  /**
   * <code>optional string errorMessage = 2;</code>
   */
  java.lang.String getErrorMessage();
  /**
   * <code>optional string errorMessage = 2;</code>
   */
  com.google.protobuf.ByteString
      getErrorMessageBytes();

  /**
   * <code>optional .alluxio.grpc.job.Status status = 4;</code>
   */
  boolean hasStatus();
  /**
   * <code>optional .alluxio.grpc.job.Status status = 4;</code>
   */
  alluxio.grpc.Status getStatus();

  /**
   * <code>optional int64 lastUpdated = 6;</code>
   */
  boolean hasLastUpdated();
  /**
   * <code>optional int64 lastUpdated = 6;</code>
   */
  long getLastUpdated();

  /**
   * <code>optional .alluxio.grpc.job.JobType type = 8;</code>
   */
  boolean hasType();
  /**
   * <code>optional .alluxio.grpc.job.JobType type = 8;</code>
   */
  alluxio.grpc.JobType getType();

  /**
   * <code>optional bytes result = 9;</code>
   */
  boolean hasResult();
  /**
   * <code>optional bytes result = 9;</code>
   */
  com.google.protobuf.ByteString getResult();

  /**
   * <pre>
   * Some jobs don't have these do not have these
   * </pre>
   *
   * <code>optional string name = 7;</code>
   */
  boolean hasName();
  /**
   * <pre>
   * Some jobs don't have these do not have these
   * </pre>
   *
   * <code>optional string name = 7;</code>
   */
  java.lang.String getName();
  /**
   * <pre>
   * Some jobs don't have these do not have these
   * </pre>
   *
   * <code>optional string name = 7;</code>
   */
  com.google.protobuf.ByteString
      getNameBytes();

  /**
   * <code>optional int64 parentId = 10;</code>
   */
  boolean hasParentId();
  /**
   * <code>optional int64 parentId = 10;</code>
   */
  long getParentId();

  /**
   * <code>repeated .alluxio.grpc.job.JobInfo children = 11;</code>
   */
  java.util.List<alluxio.grpc.JobInfo> 
      getChildrenList();
  /**
   * <code>repeated .alluxio.grpc.job.JobInfo children = 11;</code>
   */
  alluxio.grpc.JobInfo getChildren(int index);
  /**
   * <code>repeated .alluxio.grpc.job.JobInfo children = 11;</code>
   */
  int getChildrenCount();
  /**
   * <code>repeated .alluxio.grpc.job.JobInfo children = 11;</code>
   */
  java.util.List<? extends alluxio.grpc.JobInfoOrBuilder> 
      getChildrenOrBuilderList();
  /**
   * <code>repeated .alluxio.grpc.job.JobInfo children = 11;</code>
   */
  alluxio.grpc.JobInfoOrBuilder getChildrenOrBuilder(
      int index);

  /**
   * <pre>
   * Around for backwards compatibility
   * </pre>
   *
   * <code>repeated .alluxio.grpc.job.JobUnused unused0 = 3;</code>
   */
  java.util.List<alluxio.grpc.JobUnused> 
      getUnused0List();
  /**
   * <pre>
   * Around for backwards compatibility
   * </pre>
   *
   * <code>repeated .alluxio.grpc.job.JobUnused unused0 = 3;</code>
   */
  alluxio.grpc.JobUnused getUnused0(int index);
  /**
   * <pre>
   * Around for backwards compatibility
   * </pre>
   *
   * <code>repeated .alluxio.grpc.job.JobUnused unused0 = 3;</code>
   */
  int getUnused0Count();
  /**
   * <pre>
   * Around for backwards compatibility
   * </pre>
   *
   * <code>repeated .alluxio.grpc.job.JobUnused unused0 = 3;</code>
   */
  java.util.List<? extends alluxio.grpc.JobUnusedOrBuilder> 
      getUnused0OrBuilderList();
  /**
   * <pre>
   * Around for backwards compatibility
   * </pre>
   *
   * <code>repeated .alluxio.grpc.job.JobUnused unused0 = 3;</code>
   */
  alluxio.grpc.JobUnusedOrBuilder getUnused0OrBuilder(
      int index);

  /**
   * <pre>
   * formerly result
   * </pre>
   *
   * <code>optional string unused1 = 5;</code>
   */
  boolean hasUnused1();
  /**
   * <pre>
   * formerly result
   * </pre>
   *
   * <code>optional string unused1 = 5;</code>
   */
  java.lang.String getUnused1();
  /**
   * <pre>
   * formerly result
   * </pre>
   *
   * <code>optional string unused1 = 5;</code>
   */
  com.google.protobuf.ByteString
      getUnused1Bytes();
}

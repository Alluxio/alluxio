// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.job.JobInfo}
 */
public  final class JobInfo extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.job.JobInfo)
    JobInfoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use JobInfo.newBuilder() to construct.
  private JobInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private JobInfo() {
    id_ = 0L;
    errorMessage_ = "";
    taskInfos_ = java.util.Collections.emptyList();
    status_ = 0;
    result_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private JobInfo(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 8: {
            bitField0_ |= 0x00000001;
            id_ = input.readInt64();
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            errorMessage_ = bs;
            break;
          }
          case 26: {
            if (!((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
              taskInfos_ = new java.util.ArrayList<alluxio.grpc.TaskInfo>();
              mutable_bitField0_ |= 0x00000004;
            }
            taskInfos_.add(
                input.readMessage(alluxio.grpc.TaskInfo.PARSER, extensionRegistry));
            break;
          }
          case 32: {
            int rawValue = input.readEnum();
            alluxio.grpc.Status value = alluxio.grpc.Status.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(4, rawValue);
            } else {
              bitField0_ |= 0x00000004;
              status_ = rawValue;
            }
            break;
          }
          case 42: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000008;
            result_ = bs;
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000004) == 0x00000004)) {
        taskInfos_ = java.util.Collections.unmodifiableList(taskInfos_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_JobInfo_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_JobInfo_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.JobInfo.class, alluxio.grpc.JobInfo.Builder.class);
  }

  private int bitField0_;
  public static final int ID_FIELD_NUMBER = 1;
  private long id_;
  /**
   * <code>optional int64 id = 1;</code>
   */
  public boolean hasId() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional int64 id = 1;</code>
   */
  public long getId() {
    return id_;
  }

  public static final int ERRORMESSAGE_FIELD_NUMBER = 2;
  private volatile java.lang.Object errorMessage_;
  /**
   * <code>optional string errorMessage = 2;</code>
   */
  public boolean hasErrorMessage() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string errorMessage = 2;</code>
   */
  public java.lang.String getErrorMessage() {
    java.lang.Object ref = errorMessage_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        errorMessage_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string errorMessage = 2;</code>
   */
  public com.google.protobuf.ByteString
      getErrorMessageBytes() {
    java.lang.Object ref = errorMessage_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      errorMessage_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TASKINFOS_FIELD_NUMBER = 3;
  private java.util.List<alluxio.grpc.TaskInfo> taskInfos_;
  /**
   * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
   */
  public java.util.List<alluxio.grpc.TaskInfo> getTaskInfosList() {
    return taskInfos_;
  }
  /**
   * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
   */
  public java.util.List<? extends alluxio.grpc.TaskInfoOrBuilder> 
      getTaskInfosOrBuilderList() {
    return taskInfos_;
  }
  /**
   * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
   */
  public int getTaskInfosCount() {
    return taskInfos_.size();
  }
  /**
   * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
   */
  public alluxio.grpc.TaskInfo getTaskInfos(int index) {
    return taskInfos_.get(index);
  }
  /**
   * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
   */
  public alluxio.grpc.TaskInfoOrBuilder getTaskInfosOrBuilder(
      int index) {
    return taskInfos_.get(index);
  }

  public static final int STATUS_FIELD_NUMBER = 4;
  private int status_;
  /**
   * <code>optional .alluxio.grpc.job.Status status = 4;</code>
   */
  public boolean hasStatus() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional .alluxio.grpc.job.Status status = 4;</code>
   */
  public alluxio.grpc.Status getStatus() {
    alluxio.grpc.Status result = alluxio.grpc.Status.valueOf(status_);
    return result == null ? alluxio.grpc.Status.UNKNOWN : result;
  }

  public static final int RESULT_FIELD_NUMBER = 5;
  private volatile java.lang.Object result_;
  /**
   * <code>optional string result = 5;</code>
   */
  public boolean hasResult() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional string result = 5;</code>
   */
  public java.lang.String getResult() {
    java.lang.Object ref = result_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        result_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string result = 5;</code>
   */
  public com.google.protobuf.ByteString
      getResultBytes() {
    java.lang.Object ref = result_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      result_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      output.writeInt64(1, id_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, errorMessage_);
    }
    for (int i = 0; i < taskInfos_.size(); i++) {
      output.writeMessage(3, taskInfos_.get(i));
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeEnum(4, status_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 5, result_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, id_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, errorMessage_);
    }
    for (int i = 0; i < taskInfos_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, taskInfos_.get(i));
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(4, status_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(5, result_);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof alluxio.grpc.JobInfo)) {
      return super.equals(obj);
    }
    alluxio.grpc.JobInfo other = (alluxio.grpc.JobInfo) obj;

    boolean result = true;
    result = result && (hasId() == other.hasId());
    if (hasId()) {
      result = result && (getId()
          == other.getId());
    }
    result = result && (hasErrorMessage() == other.hasErrorMessage());
    if (hasErrorMessage()) {
      result = result && getErrorMessage()
          .equals(other.getErrorMessage());
    }
    result = result && getTaskInfosList()
        .equals(other.getTaskInfosList());
    result = result && (hasStatus() == other.hasStatus());
    if (hasStatus()) {
      result = result && status_ == other.status_;
    }
    result = result && (hasResult() == other.hasResult());
    if (hasResult()) {
      result = result && getResult()
          .equals(other.getResult());
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasId()) {
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getId());
    }
    if (hasErrorMessage()) {
      hash = (37 * hash) + ERRORMESSAGE_FIELD_NUMBER;
      hash = (53 * hash) + getErrorMessage().hashCode();
    }
    if (getTaskInfosCount() > 0) {
      hash = (37 * hash) + TASKINFOS_FIELD_NUMBER;
      hash = (53 * hash) + getTaskInfosList().hashCode();
    }
    if (hasStatus()) {
      hash = (37 * hash) + STATUS_FIELD_NUMBER;
      hash = (53 * hash) + status_;
    }
    if (hasResult()) {
      hash = (37 * hash) + RESULT_FIELD_NUMBER;
      hash = (53 * hash) + getResult().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.JobInfo parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.JobInfo parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.JobInfo parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.JobInfo parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.JobInfo parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.JobInfo parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(alluxio.grpc.JobInfo prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code alluxio.grpc.job.JobInfo}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.job.JobInfo)
      alluxio.grpc.JobInfoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_JobInfo_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_JobInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.JobInfo.class, alluxio.grpc.JobInfo.Builder.class);
    }

    // Construct using alluxio.grpc.JobInfo.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getTaskInfosFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      id_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      errorMessage_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      if (taskInfosBuilder_ == null) {
        taskInfos_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
      } else {
        taskInfosBuilder_.clear();
      }
      status_ = 0;
      bitField0_ = (bitField0_ & ~0x00000008);
      result_ = "";
      bitField0_ = (bitField0_ & ~0x00000010);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_JobInfo_descriptor;
    }

    public alluxio.grpc.JobInfo getDefaultInstanceForType() {
      return alluxio.grpc.JobInfo.getDefaultInstance();
    }

    public alluxio.grpc.JobInfo build() {
      alluxio.grpc.JobInfo result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.JobInfo buildPartial() {
      alluxio.grpc.JobInfo result = new alluxio.grpc.JobInfo(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.id_ = id_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.errorMessage_ = errorMessage_;
      if (taskInfosBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004)) {
          taskInfos_ = java.util.Collections.unmodifiableList(taskInfos_);
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.taskInfos_ = taskInfos_;
      } else {
        result.taskInfos_ = taskInfosBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000004;
      }
      result.status_ = status_;
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000008;
      }
      result.result_ = result_;
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof alluxio.grpc.JobInfo) {
        return mergeFrom((alluxio.grpc.JobInfo)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.JobInfo other) {
      if (other == alluxio.grpc.JobInfo.getDefaultInstance()) return this;
      if (other.hasId()) {
        setId(other.getId());
      }
      if (other.hasErrorMessage()) {
        bitField0_ |= 0x00000002;
        errorMessage_ = other.errorMessage_;
        onChanged();
      }
      if (taskInfosBuilder_ == null) {
        if (!other.taskInfos_.isEmpty()) {
          if (taskInfos_.isEmpty()) {
            taskInfos_ = other.taskInfos_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensureTaskInfosIsMutable();
            taskInfos_.addAll(other.taskInfos_);
          }
          onChanged();
        }
      } else {
        if (!other.taskInfos_.isEmpty()) {
          if (taskInfosBuilder_.isEmpty()) {
            taskInfosBuilder_.dispose();
            taskInfosBuilder_ = null;
            taskInfos_ = other.taskInfos_;
            bitField0_ = (bitField0_ & ~0x00000004);
            taskInfosBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getTaskInfosFieldBuilder() : null;
          } else {
            taskInfosBuilder_.addAllMessages(other.taskInfos_);
          }
        }
      }
      if (other.hasStatus()) {
        setStatus(other.getStatus());
      }
      if (other.hasResult()) {
        bitField0_ |= 0x00000010;
        result_ = other.result_;
        onChanged();
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      alluxio.grpc.JobInfo parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.JobInfo) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private long id_ ;
    /**
     * <code>optional int64 id = 1;</code>
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 id = 1;</code>
     */
    public long getId() {
      return id_;
    }
    /**
     * <code>optional int64 id = 1;</code>
     */
    public Builder setId(long value) {
      bitField0_ |= 0x00000001;
      id_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 id = 1;</code>
     */
    public Builder clearId() {
      bitField0_ = (bitField0_ & ~0x00000001);
      id_ = 0L;
      onChanged();
      return this;
    }

    private java.lang.Object errorMessage_ = "";
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public boolean hasErrorMessage() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public java.lang.String getErrorMessage() {
      java.lang.Object ref = errorMessage_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          errorMessage_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public com.google.protobuf.ByteString
        getErrorMessageBytes() {
      java.lang.Object ref = errorMessage_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        errorMessage_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public Builder setErrorMessage(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      errorMessage_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public Builder clearErrorMessage() {
      bitField0_ = (bitField0_ & ~0x00000002);
      errorMessage_ = getDefaultInstance().getErrorMessage();
      onChanged();
      return this;
    }
    /**
     * <code>optional string errorMessage = 2;</code>
     */
    public Builder setErrorMessageBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      errorMessage_ = value;
      onChanged();
      return this;
    }

    private java.util.List<alluxio.grpc.TaskInfo> taskInfos_ =
      java.util.Collections.emptyList();
    private void ensureTaskInfosIsMutable() {
      if (!((bitField0_ & 0x00000004) == 0x00000004)) {
        taskInfos_ = new java.util.ArrayList<alluxio.grpc.TaskInfo>(taskInfos_);
        bitField0_ |= 0x00000004;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.TaskInfo, alluxio.grpc.TaskInfo.Builder, alluxio.grpc.TaskInfoOrBuilder> taskInfosBuilder_;

    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public java.util.List<alluxio.grpc.TaskInfo> getTaskInfosList() {
      if (taskInfosBuilder_ == null) {
        return java.util.Collections.unmodifiableList(taskInfos_);
      } else {
        return taskInfosBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public int getTaskInfosCount() {
      if (taskInfosBuilder_ == null) {
        return taskInfos_.size();
      } else {
        return taskInfosBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public alluxio.grpc.TaskInfo getTaskInfos(int index) {
      if (taskInfosBuilder_ == null) {
        return taskInfos_.get(index);
      } else {
        return taskInfosBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder setTaskInfos(
        int index, alluxio.grpc.TaskInfo value) {
      if (taskInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTaskInfosIsMutable();
        taskInfos_.set(index, value);
        onChanged();
      } else {
        taskInfosBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder setTaskInfos(
        int index, alluxio.grpc.TaskInfo.Builder builderForValue) {
      if (taskInfosBuilder_ == null) {
        ensureTaskInfosIsMutable();
        taskInfos_.set(index, builderForValue.build());
        onChanged();
      } else {
        taskInfosBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder addTaskInfos(alluxio.grpc.TaskInfo value) {
      if (taskInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTaskInfosIsMutable();
        taskInfos_.add(value);
        onChanged();
      } else {
        taskInfosBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder addTaskInfos(
        int index, alluxio.grpc.TaskInfo value) {
      if (taskInfosBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTaskInfosIsMutable();
        taskInfos_.add(index, value);
        onChanged();
      } else {
        taskInfosBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder addTaskInfos(
        alluxio.grpc.TaskInfo.Builder builderForValue) {
      if (taskInfosBuilder_ == null) {
        ensureTaskInfosIsMutable();
        taskInfos_.add(builderForValue.build());
        onChanged();
      } else {
        taskInfosBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder addTaskInfos(
        int index, alluxio.grpc.TaskInfo.Builder builderForValue) {
      if (taskInfosBuilder_ == null) {
        ensureTaskInfosIsMutable();
        taskInfos_.add(index, builderForValue.build());
        onChanged();
      } else {
        taskInfosBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder addAllTaskInfos(
        java.lang.Iterable<? extends alluxio.grpc.TaskInfo> values) {
      if (taskInfosBuilder_ == null) {
        ensureTaskInfosIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, taskInfos_);
        onChanged();
      } else {
        taskInfosBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder clearTaskInfos() {
      if (taskInfosBuilder_ == null) {
        taskInfos_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
      } else {
        taskInfosBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public Builder removeTaskInfos(int index) {
      if (taskInfosBuilder_ == null) {
        ensureTaskInfosIsMutable();
        taskInfos_.remove(index);
        onChanged();
      } else {
        taskInfosBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public alluxio.grpc.TaskInfo.Builder getTaskInfosBuilder(
        int index) {
      return getTaskInfosFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public alluxio.grpc.TaskInfoOrBuilder getTaskInfosOrBuilder(
        int index) {
      if (taskInfosBuilder_ == null) {
        return taskInfos_.get(index);  } else {
        return taskInfosBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public java.util.List<? extends alluxio.grpc.TaskInfoOrBuilder> 
         getTaskInfosOrBuilderList() {
      if (taskInfosBuilder_ != null) {
        return taskInfosBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(taskInfos_);
      }
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public alluxio.grpc.TaskInfo.Builder addTaskInfosBuilder() {
      return getTaskInfosFieldBuilder().addBuilder(
          alluxio.grpc.TaskInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public alluxio.grpc.TaskInfo.Builder addTaskInfosBuilder(
        int index) {
      return getTaskInfosFieldBuilder().addBuilder(
          index, alluxio.grpc.TaskInfo.getDefaultInstance());
    }
    /**
     * <code>repeated .alluxio.grpc.job.TaskInfo taskInfos = 3;</code>
     */
    public java.util.List<alluxio.grpc.TaskInfo.Builder> 
         getTaskInfosBuilderList() {
      return getTaskInfosFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        alluxio.grpc.TaskInfo, alluxio.grpc.TaskInfo.Builder, alluxio.grpc.TaskInfoOrBuilder> 
        getTaskInfosFieldBuilder() {
      if (taskInfosBuilder_ == null) {
        taskInfosBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            alluxio.grpc.TaskInfo, alluxio.grpc.TaskInfo.Builder, alluxio.grpc.TaskInfoOrBuilder>(
                taskInfos_,
                ((bitField0_ & 0x00000004) == 0x00000004),
                getParentForChildren(),
                isClean());
        taskInfos_ = null;
      }
      return taskInfosBuilder_;
    }

    private int status_ = 0;
    /**
     * <code>optional .alluxio.grpc.job.Status status = 4;</code>
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional .alluxio.grpc.job.Status status = 4;</code>
     */
    public alluxio.grpc.Status getStatus() {
      alluxio.grpc.Status result = alluxio.grpc.Status.valueOf(status_);
      return result == null ? alluxio.grpc.Status.UNKNOWN : result;
    }
    /**
     * <code>optional .alluxio.grpc.job.Status status = 4;</code>
     */
    public Builder setStatus(alluxio.grpc.Status value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000008;
      status_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.Status status = 4;</code>
     */
    public Builder clearStatus() {
      bitField0_ = (bitField0_ & ~0x00000008);
      status_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object result_ = "";
    /**
     * <code>optional string result = 5;</code>
     */
    public boolean hasResult() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional string result = 5;</code>
     */
    public java.lang.String getResult() {
      java.lang.Object ref = result_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          result_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string result = 5;</code>
     */
    public com.google.protobuf.ByteString
        getResultBytes() {
      java.lang.Object ref = result_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        result_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string result = 5;</code>
     */
    public Builder setResult(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000010;
      result_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string result = 5;</code>
     */
    public Builder clearResult() {
      bitField0_ = (bitField0_ & ~0x00000010);
      result_ = getDefaultInstance().getResult();
      onChanged();
      return this;
    }
    /**
     * <code>optional string result = 5;</code>
     */
    public Builder setResultBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000010;
      result_ = value;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.job.JobInfo)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.job.JobInfo)
  private static final alluxio.grpc.JobInfo DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.JobInfo();
  }

  public static alluxio.grpc.JobInfo getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<JobInfo>
      PARSER = new com.google.protobuf.AbstractParser<JobInfo>() {
    public JobInfo parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new JobInfo(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<JobInfo> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<JobInfo> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.JobInfo getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


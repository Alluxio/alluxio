// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.job.CancelTaskCommand}
 */
public  final class CancelTaskCommand extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.job.CancelTaskCommand)
    CancelTaskCommandOrBuilder {
private static final long serialVersionUID = 0L;
  // Use CancelTaskCommand.newBuilder() to construct.
  private CancelTaskCommand(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private CancelTaskCommand() {
    jobId_ = 0L;
    taskId_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private CancelTaskCommand(
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
            jobId_ = input.readInt64();
            break;
          }
          case 16: {
            bitField0_ |= 0x00000002;
            taskId_ = input.readInt64();
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
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_CancelTaskCommand_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_CancelTaskCommand_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.CancelTaskCommand.class, alluxio.grpc.CancelTaskCommand.Builder.class);
  }

  private int bitField0_;
  public static final int JOBID_FIELD_NUMBER = 1;
  private long jobId_;
  /**
   * <code>optional int64 jobId = 1;</code>
   */
  public boolean hasJobId() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional int64 jobId = 1;</code>
   */
  public long getJobId() {
    return jobId_;
  }

  public static final int TASKID_FIELD_NUMBER = 2;
  private long taskId_;
  /**
   * <code>optional int64 taskId = 2;</code>
   */
  public boolean hasTaskId() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional int64 taskId = 2;</code>
   */
  public long getTaskId() {
    return taskId_;
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
      output.writeInt64(1, jobId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeInt64(2, taskId_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, jobId_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(2, taskId_);
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
    if (!(obj instanceof alluxio.grpc.CancelTaskCommand)) {
      return super.equals(obj);
    }
    alluxio.grpc.CancelTaskCommand other = (alluxio.grpc.CancelTaskCommand) obj;

    boolean result = true;
    result = result && (hasJobId() == other.hasJobId());
    if (hasJobId()) {
      result = result && (getJobId()
          == other.getJobId());
    }
    result = result && (hasTaskId() == other.hasTaskId());
    if (hasTaskId()) {
      result = result && (getTaskId()
          == other.getTaskId());
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
    if (hasJobId()) {
      hash = (37 * hash) + JOBID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getJobId());
    }
    if (hasTaskId()) {
      hash = (37 * hash) + TASKID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getTaskId());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.CancelTaskCommand parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.CancelTaskCommand parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CancelTaskCommand parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.CancelTaskCommand parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.CancelTaskCommand prototype) {
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
   * Protobuf type {@code alluxio.grpc.job.CancelTaskCommand}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.job.CancelTaskCommand)
      alluxio.grpc.CancelTaskCommandOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_CancelTaskCommand_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_CancelTaskCommand_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.CancelTaskCommand.class, alluxio.grpc.CancelTaskCommand.Builder.class);
    }

    // Construct using alluxio.grpc.CancelTaskCommand.newBuilder()
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
      }
    }
    public Builder clear() {
      super.clear();
      jobId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      taskId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.JobMasterProto.internal_static_alluxio_grpc_job_CancelTaskCommand_descriptor;
    }

    public alluxio.grpc.CancelTaskCommand getDefaultInstanceForType() {
      return alluxio.grpc.CancelTaskCommand.getDefaultInstance();
    }

    public alluxio.grpc.CancelTaskCommand build() {
      alluxio.grpc.CancelTaskCommand result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.CancelTaskCommand buildPartial() {
      alluxio.grpc.CancelTaskCommand result = new alluxio.grpc.CancelTaskCommand(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.jobId_ = jobId_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.taskId_ = taskId_;
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
      if (other instanceof alluxio.grpc.CancelTaskCommand) {
        return mergeFrom((alluxio.grpc.CancelTaskCommand)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.CancelTaskCommand other) {
      if (other == alluxio.grpc.CancelTaskCommand.getDefaultInstance()) return this;
      if (other.hasJobId()) {
        setJobId(other.getJobId());
      }
      if (other.hasTaskId()) {
        setTaskId(other.getTaskId());
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
      alluxio.grpc.CancelTaskCommand parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.CancelTaskCommand) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private long jobId_ ;
    /**
     * <code>optional int64 jobId = 1;</code>
     */
    public boolean hasJobId() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 jobId = 1;</code>
     */
    public long getJobId() {
      return jobId_;
    }
    /**
     * <code>optional int64 jobId = 1;</code>
     */
    public Builder setJobId(long value) {
      bitField0_ |= 0x00000001;
      jobId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 jobId = 1;</code>
     */
    public Builder clearJobId() {
      bitField0_ = (bitField0_ & ~0x00000001);
      jobId_ = 0L;
      onChanged();
      return this;
    }

    private long taskId_ ;
    /**
     * <code>optional int64 taskId = 2;</code>
     */
    public boolean hasTaskId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int64 taskId = 2;</code>
     */
    public long getTaskId() {
      return taskId_;
    }
    /**
     * <code>optional int64 taskId = 2;</code>
     */
    public Builder setTaskId(long value) {
      bitField0_ |= 0x00000002;
      taskId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 taskId = 2;</code>
     */
    public Builder clearTaskId() {
      bitField0_ = (bitField0_ & ~0x00000002);
      taskId_ = 0L;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.job.CancelTaskCommand)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.job.CancelTaskCommand)
  private static final alluxio.grpc.CancelTaskCommand DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.CancelTaskCommand();
  }

  public static alluxio.grpc.CancelTaskCommand getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<CancelTaskCommand>
      PARSER = new com.google.protobuf.AbstractParser<CancelTaskCommand>() {
    public CancelTaskCommand parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new CancelTaskCommand(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<CancelTaskCommand> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<CancelTaskCommand> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.CancelTaskCommand getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


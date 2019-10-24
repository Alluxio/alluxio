// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/table/table_master.proto

package alluxio.grpc.table;

/**
 * Protobuf type {@code alluxio.grpc.table.TransformJobInfo}
 */
public  final class TransformJobInfo extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.table.TransformJobInfo)
    TransformJobInfoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use TransformJobInfo.newBuilder() to construct.
  private TransformJobInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private TransformJobInfo() {
    dbName_ = "";
    tableName_ = "";
    definition_ = "";
    jobId_ = 0L;
    jobStatus_ = 0;
    jobError_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private TransformJobInfo(
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
          case 10: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000001;
            dbName_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            tableName_ = bs;
            break;
          }
          case 26: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000004;
            definition_ = bs;
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            jobId_ = input.readInt64();
            break;
          }
          case 40: {
            int rawValue = input.readEnum();
            alluxio.grpc.Status value = alluxio.grpc.Status.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(5, rawValue);
            } else {
              bitField0_ |= 0x00000010;
              jobStatus_ = rawValue;
            }
            break;
          }
          case 50: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000020;
            jobError_ = bs;
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
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_TransformJobInfo_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_TransformJobInfo_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.table.TransformJobInfo.class, alluxio.grpc.table.TransformJobInfo.Builder.class);
  }

  private int bitField0_;
  public static final int DB_NAME_FIELD_NUMBER = 1;
  private volatile java.lang.Object dbName_;
  /**
   * <code>optional string db_name = 1;</code>
   */
  public boolean hasDbName() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string db_name = 1;</code>
   */
  public java.lang.String getDbName() {
    java.lang.Object ref = dbName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        dbName_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string db_name = 1;</code>
   */
  public com.google.protobuf.ByteString
      getDbNameBytes() {
    java.lang.Object ref = dbName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      dbName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TABLE_NAME_FIELD_NUMBER = 2;
  private volatile java.lang.Object tableName_;
  /**
   * <code>optional string table_name = 2;</code>
   */
  public boolean hasTableName() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional string table_name = 2;</code>
   */
  public java.lang.String getTableName() {
    java.lang.Object ref = tableName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        tableName_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string table_name = 2;</code>
   */
  public com.google.protobuf.ByteString
      getTableNameBytes() {
    java.lang.Object ref = tableName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      tableName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int DEFINITION_FIELD_NUMBER = 3;
  private volatile java.lang.Object definition_;
  /**
   * <code>optional string definition = 3;</code>
   */
  public boolean hasDefinition() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional string definition = 3;</code>
   */
  public java.lang.String getDefinition() {
    java.lang.Object ref = definition_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        definition_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string definition = 3;</code>
   */
  public com.google.protobuf.ByteString
      getDefinitionBytes() {
    java.lang.Object ref = definition_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      definition_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int JOB_ID_FIELD_NUMBER = 4;
  private long jobId_;
  /**
   * <code>optional int64 job_id = 4;</code>
   */
  public boolean hasJobId() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional int64 job_id = 4;</code>
   */
  public long getJobId() {
    return jobId_;
  }

  public static final int JOB_STATUS_FIELD_NUMBER = 5;
  private int jobStatus_;
  /**
   * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
   */
  public boolean hasJobStatus() {
    return ((bitField0_ & 0x00000010) == 0x00000010);
  }
  /**
   * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
   */
  public alluxio.grpc.Status getJobStatus() {
    alluxio.grpc.Status result = alluxio.grpc.Status.valueOf(jobStatus_);
    return result == null ? alluxio.grpc.Status.UNKNOWN : result;
  }

  public static final int JOB_ERROR_FIELD_NUMBER = 6;
  private volatile java.lang.Object jobError_;
  /**
   * <code>optional string job_error = 6;</code>
   */
  public boolean hasJobError() {
    return ((bitField0_ & 0x00000020) == 0x00000020);
  }
  /**
   * <code>optional string job_error = 6;</code>
   */
  public java.lang.String getJobError() {
    java.lang.Object ref = jobError_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        jobError_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string job_error = 6;</code>
   */
  public com.google.protobuf.ByteString
      getJobErrorBytes() {
    java.lang.Object ref = jobError_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      jobError_ = b;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, dbName_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, tableName_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, definition_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeInt64(4, jobId_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      output.writeEnum(5, jobStatus_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 6, jobError_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, dbName_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, tableName_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, definition_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(4, jobId_);
    }
    if (((bitField0_ & 0x00000010) == 0x00000010)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(5, jobStatus_);
    }
    if (((bitField0_ & 0x00000020) == 0x00000020)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(6, jobError_);
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
    if (!(obj instanceof alluxio.grpc.table.TransformJobInfo)) {
      return super.equals(obj);
    }
    alluxio.grpc.table.TransformJobInfo other = (alluxio.grpc.table.TransformJobInfo) obj;

    boolean result = true;
    result = result && (hasDbName() == other.hasDbName());
    if (hasDbName()) {
      result = result && getDbName()
          .equals(other.getDbName());
    }
    result = result && (hasTableName() == other.hasTableName());
    if (hasTableName()) {
      result = result && getTableName()
          .equals(other.getTableName());
    }
    result = result && (hasDefinition() == other.hasDefinition());
    if (hasDefinition()) {
      result = result && getDefinition()
          .equals(other.getDefinition());
    }
    result = result && (hasJobId() == other.hasJobId());
    if (hasJobId()) {
      result = result && (getJobId()
          == other.getJobId());
    }
    result = result && (hasJobStatus() == other.hasJobStatus());
    if (hasJobStatus()) {
      result = result && jobStatus_ == other.jobStatus_;
    }
    result = result && (hasJobError() == other.hasJobError());
    if (hasJobError()) {
      result = result && getJobError()
          .equals(other.getJobError());
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
    if (hasDbName()) {
      hash = (37 * hash) + DB_NAME_FIELD_NUMBER;
      hash = (53 * hash) + getDbName().hashCode();
    }
    if (hasTableName()) {
      hash = (37 * hash) + TABLE_NAME_FIELD_NUMBER;
      hash = (53 * hash) + getTableName().hashCode();
    }
    if (hasDefinition()) {
      hash = (37 * hash) + DEFINITION_FIELD_NUMBER;
      hash = (53 * hash) + getDefinition().hashCode();
    }
    if (hasJobId()) {
      hash = (37 * hash) + JOB_ID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getJobId());
    }
    if (hasJobStatus()) {
      hash = (37 * hash) + JOB_STATUS_FIELD_NUMBER;
      hash = (53 * hash) + jobStatus_;
    }
    if (hasJobError()) {
      hash = (37 * hash) + JOB_ERROR_FIELD_NUMBER;
      hash = (53 * hash) + getJobError().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.TransformJobInfo parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.TransformJobInfo parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.table.TransformJobInfo parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.table.TransformJobInfo prototype) {
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
   * Protobuf type {@code alluxio.grpc.table.TransformJobInfo}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.table.TransformJobInfo)
      alluxio.grpc.table.TransformJobInfoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_TransformJobInfo_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_TransformJobInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.table.TransformJobInfo.class, alluxio.grpc.table.TransformJobInfo.Builder.class);
    }

    // Construct using alluxio.grpc.table.TransformJobInfo.newBuilder()
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
      dbName_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      tableName_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      definition_ = "";
      bitField0_ = (bitField0_ & ~0x00000004);
      jobId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000008);
      jobStatus_ = 0;
      bitField0_ = (bitField0_ & ~0x00000010);
      jobError_ = "";
      bitField0_ = (bitField0_ & ~0x00000020);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.table.TableMasterProto.internal_static_alluxio_grpc_table_TransformJobInfo_descriptor;
    }

    public alluxio.grpc.table.TransformJobInfo getDefaultInstanceForType() {
      return alluxio.grpc.table.TransformJobInfo.getDefaultInstance();
    }

    public alluxio.grpc.table.TransformJobInfo build() {
      alluxio.grpc.table.TransformJobInfo result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.table.TransformJobInfo buildPartial() {
      alluxio.grpc.table.TransformJobInfo result = new alluxio.grpc.table.TransformJobInfo(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.dbName_ = dbName_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.tableName_ = tableName_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.definition_ = definition_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.jobId_ = jobId_;
      if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
        to_bitField0_ |= 0x00000010;
      }
      result.jobStatus_ = jobStatus_;
      if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
        to_bitField0_ |= 0x00000020;
      }
      result.jobError_ = jobError_;
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
      if (other instanceof alluxio.grpc.table.TransformJobInfo) {
        return mergeFrom((alluxio.grpc.table.TransformJobInfo)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.table.TransformJobInfo other) {
      if (other == alluxio.grpc.table.TransformJobInfo.getDefaultInstance()) return this;
      if (other.hasDbName()) {
        bitField0_ |= 0x00000001;
        dbName_ = other.dbName_;
        onChanged();
      }
      if (other.hasTableName()) {
        bitField0_ |= 0x00000002;
        tableName_ = other.tableName_;
        onChanged();
      }
      if (other.hasDefinition()) {
        bitField0_ |= 0x00000004;
        definition_ = other.definition_;
        onChanged();
      }
      if (other.hasJobId()) {
        setJobId(other.getJobId());
      }
      if (other.hasJobStatus()) {
        setJobStatus(other.getJobStatus());
      }
      if (other.hasJobError()) {
        bitField0_ |= 0x00000020;
        jobError_ = other.jobError_;
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
      alluxio.grpc.table.TransformJobInfo parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.table.TransformJobInfo) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object dbName_ = "";
    /**
     * <code>optional string db_name = 1;</code>
     */
    public boolean hasDbName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string db_name = 1;</code>
     */
    public java.lang.String getDbName() {
      java.lang.Object ref = dbName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          dbName_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string db_name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getDbNameBytes() {
      java.lang.Object ref = dbName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        dbName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string db_name = 1;</code>
     */
    public Builder setDbName(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      dbName_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string db_name = 1;</code>
     */
    public Builder clearDbName() {
      bitField0_ = (bitField0_ & ~0x00000001);
      dbName_ = getDefaultInstance().getDbName();
      onChanged();
      return this;
    }
    /**
     * <code>optional string db_name = 1;</code>
     */
    public Builder setDbNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      dbName_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object tableName_ = "";
    /**
     * <code>optional string table_name = 2;</code>
     */
    public boolean hasTableName() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string table_name = 2;</code>
     */
    public java.lang.String getTableName() {
      java.lang.Object ref = tableName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          tableName_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string table_name = 2;</code>
     */
    public com.google.protobuf.ByteString
        getTableNameBytes() {
      java.lang.Object ref = tableName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tableName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string table_name = 2;</code>
     */
    public Builder setTableName(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      tableName_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string table_name = 2;</code>
     */
    public Builder clearTableName() {
      bitField0_ = (bitField0_ & ~0x00000002);
      tableName_ = getDefaultInstance().getTableName();
      onChanged();
      return this;
    }
    /**
     * <code>optional string table_name = 2;</code>
     */
    public Builder setTableNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      tableName_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object definition_ = "";
    /**
     * <code>optional string definition = 3;</code>
     */
    public boolean hasDefinition() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string definition = 3;</code>
     */
    public java.lang.String getDefinition() {
      java.lang.Object ref = definition_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          definition_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string definition = 3;</code>
     */
    public com.google.protobuf.ByteString
        getDefinitionBytes() {
      java.lang.Object ref = definition_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        definition_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string definition = 3;</code>
     */
    public Builder setDefinition(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
      definition_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string definition = 3;</code>
     */
    public Builder clearDefinition() {
      bitField0_ = (bitField0_ & ~0x00000004);
      definition_ = getDefaultInstance().getDefinition();
      onChanged();
      return this;
    }
    /**
     * <code>optional string definition = 3;</code>
     */
    public Builder setDefinitionBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
      definition_ = value;
      onChanged();
      return this;
    }

    private long jobId_ ;
    /**
     * <code>optional int64 job_id = 4;</code>
     */
    public boolean hasJobId() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int64 job_id = 4;</code>
     */
    public long getJobId() {
      return jobId_;
    }
    /**
     * <code>optional int64 job_id = 4;</code>
     */
    public Builder setJobId(long value) {
      bitField0_ |= 0x00000008;
      jobId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 job_id = 4;</code>
     */
    public Builder clearJobId() {
      bitField0_ = (bitField0_ & ~0x00000008);
      jobId_ = 0L;
      onChanged();
      return this;
    }

    private int jobStatus_ = 0;
    /**
     * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
     */
    public boolean hasJobStatus() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
     */
    public alluxio.grpc.Status getJobStatus() {
      alluxio.grpc.Status result = alluxio.grpc.Status.valueOf(jobStatus_);
      return result == null ? alluxio.grpc.Status.UNKNOWN : result;
    }
    /**
     * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
     */
    public Builder setJobStatus(alluxio.grpc.Status value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000010;
      jobStatus_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.job.Status job_status = 5;</code>
     */
    public Builder clearJobStatus() {
      bitField0_ = (bitField0_ & ~0x00000010);
      jobStatus_ = 0;
      onChanged();
      return this;
    }

    private java.lang.Object jobError_ = "";
    /**
     * <code>optional string job_error = 6;</code>
     */
    public boolean hasJobError() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional string job_error = 6;</code>
     */
    public java.lang.String getJobError() {
      java.lang.Object ref = jobError_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          jobError_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string job_error = 6;</code>
     */
    public com.google.protobuf.ByteString
        getJobErrorBytes() {
      java.lang.Object ref = jobError_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        jobError_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string job_error = 6;</code>
     */
    public Builder setJobError(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
      jobError_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string job_error = 6;</code>
     */
    public Builder clearJobError() {
      bitField0_ = (bitField0_ & ~0x00000020);
      jobError_ = getDefaultInstance().getJobError();
      onChanged();
      return this;
    }
    /**
     * <code>optional string job_error = 6;</code>
     */
    public Builder setJobErrorBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
      jobError_ = value;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.table.TransformJobInfo)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.table.TransformJobInfo)
  private static final alluxio.grpc.table.TransformJobInfo DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.table.TransformJobInfo();
  }

  public static alluxio.grpc.table.TransformJobInfo getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<TransformJobInfo>
      PARSER = new com.google.protobuf.AbstractParser<TransformJobInfo>() {
    public TransformJobInfo parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new TransformJobInfo(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<TransformJobInfo> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TransformJobInfo> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.table.TransformJobInfo getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.OpenFilePOptions}
 */
public  final class OpenFilePOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.OpenFilePOptions)
    OpenFilePOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use OpenFilePOptions.newBuilder() to construct.
  private OpenFilePOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private OpenFilePOptions() {
    readType_ = 1;
    maxUfsReadConcurrency_ = 0;
    updateLastAccessTime_ = true;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private OpenFilePOptions(
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
            int rawValue = input.readEnum();
            alluxio.grpc.ReadPType value = alluxio.grpc.ReadPType.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(1, rawValue);
            } else {
              bitField0_ |= 0x00000001;
              readType_ = rawValue;
            }
            break;
          }
          case 16: {
            bitField0_ |= 0x00000002;
            maxUfsReadConcurrency_ = input.readInt32();
            break;
          }
          case 26: {
            alluxio.grpc.FileSystemMasterCommonPOptions.Builder subBuilder = null;
            if (((bitField0_ & 0x00000004) == 0x00000004)) {
              subBuilder = commonOptions_.toBuilder();
            }
            commonOptions_ = input.readMessage(alluxio.grpc.FileSystemMasterCommonPOptions.PARSER, extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(commonOptions_);
              commonOptions_ = subBuilder.buildPartial();
            }
            bitField0_ |= 0x00000004;
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            updateLastAccessTime_ = input.readBool();
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_OpenFilePOptions_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_OpenFilePOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.OpenFilePOptions.class, alluxio.grpc.OpenFilePOptions.Builder.class);
  }

  private int bitField0_;
  public static final int READTYPE_FIELD_NUMBER = 1;
  private int readType_;
  /**
   * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
   */
  public boolean hasReadType() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
   */
  public alluxio.grpc.ReadPType getReadType() {
    alluxio.grpc.ReadPType result = alluxio.grpc.ReadPType.valueOf(readType_);
    return result == null ? alluxio.grpc.ReadPType.NO_CACHE : result;
  }

  public static final int MAXUFSREADCONCURRENCY_FIELD_NUMBER = 2;
  private int maxUfsReadConcurrency_;
  /**
   * <code>optional int32 maxUfsReadConcurrency = 2;</code>
   */
  public boolean hasMaxUfsReadConcurrency() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional int32 maxUfsReadConcurrency = 2;</code>
   */
  public int getMaxUfsReadConcurrency() {
    return maxUfsReadConcurrency_;
  }

  public static final int COMMONOPTIONS_FIELD_NUMBER = 3;
  private alluxio.grpc.FileSystemMasterCommonPOptions commonOptions_;
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
   */
  public boolean hasCommonOptions() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
   */
  public alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions() {
    return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
  }
  /**
   * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
   */
  public alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder() {
    return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
  }

  public static final int UPDATELASTACCESSTIME_FIELD_NUMBER = 4;
  private boolean updateLastAccessTime_;
  /**
   * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
   */
  public boolean hasUpdateLastAccessTime() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
   */
  public boolean getUpdateLastAccessTime() {
    return updateLastAccessTime_;
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
      output.writeEnum(1, readType_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeInt32(2, maxUfsReadConcurrency_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeMessage(3, getCommonOptions());
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeBool(4, updateLastAccessTime_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(1, readType_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(2, maxUfsReadConcurrency_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, getCommonOptions());
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(4, updateLastAccessTime_);
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
    if (!(obj instanceof alluxio.grpc.OpenFilePOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.OpenFilePOptions other = (alluxio.grpc.OpenFilePOptions) obj;

    boolean result = true;
    result = result && (hasReadType() == other.hasReadType());
    if (hasReadType()) {
      result = result && readType_ == other.readType_;
    }
    result = result && (hasMaxUfsReadConcurrency() == other.hasMaxUfsReadConcurrency());
    if (hasMaxUfsReadConcurrency()) {
      result = result && (getMaxUfsReadConcurrency()
          == other.getMaxUfsReadConcurrency());
    }
    result = result && (hasCommonOptions() == other.hasCommonOptions());
    if (hasCommonOptions()) {
      result = result && getCommonOptions()
          .equals(other.getCommonOptions());
    }
    result = result && (hasUpdateLastAccessTime() == other.hasUpdateLastAccessTime());
    if (hasUpdateLastAccessTime()) {
      result = result && (getUpdateLastAccessTime()
          == other.getUpdateLastAccessTime());
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
    if (hasReadType()) {
      hash = (37 * hash) + READTYPE_FIELD_NUMBER;
      hash = (53 * hash) + readType_;
    }
    if (hasMaxUfsReadConcurrency()) {
      hash = (37 * hash) + MAXUFSREADCONCURRENCY_FIELD_NUMBER;
      hash = (53 * hash) + getMaxUfsReadConcurrency();
    }
    if (hasCommonOptions()) {
      hash = (37 * hash) + COMMONOPTIONS_FIELD_NUMBER;
      hash = (53 * hash) + getCommonOptions().hashCode();
    }
    if (hasUpdateLastAccessTime()) {
      hash = (37 * hash) + UPDATELASTACCESSTIME_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getUpdateLastAccessTime());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.OpenFilePOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.OpenFilePOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenFilePOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.OpenFilePOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.OpenFilePOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.OpenFilePOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.OpenFilePOptions)
      alluxio.grpc.OpenFilePOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_OpenFilePOptions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_OpenFilePOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.OpenFilePOptions.class, alluxio.grpc.OpenFilePOptions.Builder.class);
    }

    // Construct using alluxio.grpc.OpenFilePOptions.newBuilder()
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
        getCommonOptionsFieldBuilder();
      }
    }
    public Builder clear() {
      super.clear();
      readType_ = 1;
      bitField0_ = (bitField0_ & ~0x00000001);
      maxUfsReadConcurrency_ = 0;
      bitField0_ = (bitField0_ & ~0x00000002);
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = null;
      } else {
        commonOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      updateLastAccessTime_ = true;
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_OpenFilePOptions_descriptor;
    }

    public alluxio.grpc.OpenFilePOptions getDefaultInstanceForType() {
      return alluxio.grpc.OpenFilePOptions.getDefaultInstance();
    }

    public alluxio.grpc.OpenFilePOptions build() {
      alluxio.grpc.OpenFilePOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.OpenFilePOptions buildPartial() {
      alluxio.grpc.OpenFilePOptions result = new alluxio.grpc.OpenFilePOptions(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.readType_ = readType_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.maxUfsReadConcurrency_ = maxUfsReadConcurrency_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      if (commonOptionsBuilder_ == null) {
        result.commonOptions_ = commonOptions_;
      } else {
        result.commonOptions_ = commonOptionsBuilder_.build();
      }
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.updateLastAccessTime_ = updateLastAccessTime_;
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
      if (other instanceof alluxio.grpc.OpenFilePOptions) {
        return mergeFrom((alluxio.grpc.OpenFilePOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.OpenFilePOptions other) {
      if (other == alluxio.grpc.OpenFilePOptions.getDefaultInstance()) return this;
      if (other.hasReadType()) {
        setReadType(other.getReadType());
      }
      if (other.hasMaxUfsReadConcurrency()) {
        setMaxUfsReadConcurrency(other.getMaxUfsReadConcurrency());
      }
      if (other.hasCommonOptions()) {
        mergeCommonOptions(other.getCommonOptions());
      }
      if (other.hasUpdateLastAccessTime()) {
        setUpdateLastAccessTime(other.getUpdateLastAccessTime());
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
      alluxio.grpc.OpenFilePOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.OpenFilePOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int readType_ = 1;
    /**
     * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
     */
    public boolean hasReadType() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
     */
    public alluxio.grpc.ReadPType getReadType() {
      alluxio.grpc.ReadPType result = alluxio.grpc.ReadPType.valueOf(readType_);
      return result == null ? alluxio.grpc.ReadPType.NO_CACHE : result;
    }
    /**
     * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
     */
    public Builder setReadType(alluxio.grpc.ReadPType value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000001;
      readType_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.ReadPType readType = 1;</code>
     */
    public Builder clearReadType() {
      bitField0_ = (bitField0_ & ~0x00000001);
      readType_ = 1;
      onChanged();
      return this;
    }

    private int maxUfsReadConcurrency_ ;
    /**
     * <code>optional int32 maxUfsReadConcurrency = 2;</code>
     */
    public boolean hasMaxUfsReadConcurrency() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int32 maxUfsReadConcurrency = 2;</code>
     */
    public int getMaxUfsReadConcurrency() {
      return maxUfsReadConcurrency_;
    }
    /**
     * <code>optional int32 maxUfsReadConcurrency = 2;</code>
     */
    public Builder setMaxUfsReadConcurrency(int value) {
      bitField0_ |= 0x00000002;
      maxUfsReadConcurrency_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 maxUfsReadConcurrency = 2;</code>
     */
    public Builder clearMaxUfsReadConcurrency() {
      bitField0_ = (bitField0_ & ~0x00000002);
      maxUfsReadConcurrency_ = 0;
      onChanged();
      return this;
    }

    private alluxio.grpc.FileSystemMasterCommonPOptions commonOptions_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder> commonOptionsBuilder_;
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public boolean hasCommonOptions() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptions getCommonOptions() {
      if (commonOptionsBuilder_ == null) {
        return commonOptions_ == null ? alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
      } else {
        return commonOptionsBuilder_.getMessage();
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public Builder setCommonOptions(alluxio.grpc.FileSystemMasterCommonPOptions value) {
      if (commonOptionsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        commonOptions_ = value;
        onChanged();
      } else {
        commonOptionsBuilder_.setMessage(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public Builder setCommonOptions(
        alluxio.grpc.FileSystemMasterCommonPOptions.Builder builderForValue) {
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = builderForValue.build();
        onChanged();
      } else {
        commonOptionsBuilder_.setMessage(builderForValue.build());
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public Builder mergeCommonOptions(alluxio.grpc.FileSystemMasterCommonPOptions value) {
      if (commonOptionsBuilder_ == null) {
        if (((bitField0_ & 0x00000004) == 0x00000004) &&
            commonOptions_ != null &&
            commonOptions_ != alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance()) {
          commonOptions_ =
            alluxio.grpc.FileSystemMasterCommonPOptions.newBuilder(commonOptions_).mergeFrom(value).buildPartial();
        } else {
          commonOptions_ = value;
        }
        onChanged();
      } else {
        commonOptionsBuilder_.mergeFrom(value);
      }
      bitField0_ |= 0x00000004;
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public Builder clearCommonOptions() {
      if (commonOptionsBuilder_ == null) {
        commonOptions_ = null;
        onChanged();
      } else {
        commonOptionsBuilder_.clear();
      }
      bitField0_ = (bitField0_ & ~0x00000004);
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptions.Builder getCommonOptionsBuilder() {
      bitField0_ |= 0x00000004;
      onChanged();
      return getCommonOptionsFieldBuilder().getBuilder();
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    public alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder getCommonOptionsOrBuilder() {
      if (commonOptionsBuilder_ != null) {
        return commonOptionsBuilder_.getMessageOrBuilder();
      } else {
        return commonOptions_ == null ?
            alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance() : commonOptions_;
      }
    }
    /**
     * <code>optional .alluxio.grpc.file.FileSystemMasterCommonPOptions commonOptions = 3;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder> 
        getCommonOptionsFieldBuilder() {
      if (commonOptionsBuilder_ == null) {
        commonOptionsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            alluxio.grpc.FileSystemMasterCommonPOptions, alluxio.grpc.FileSystemMasterCommonPOptions.Builder, alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder>(
                getCommonOptions(),
                getParentForChildren(),
                isClean());
        commonOptions_ = null;
      }
      return commonOptionsBuilder_;
    }

    private boolean updateLastAccessTime_ = true;
    /**
     * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
     */
    public boolean hasUpdateLastAccessTime() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
     */
    public boolean getUpdateLastAccessTime() {
      return updateLastAccessTime_;
    }
    /**
     * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
     */
    public Builder setUpdateLastAccessTime(boolean value) {
      bitField0_ |= 0x00000008;
      updateLastAccessTime_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bool updateLastAccessTime = 4 [default = true];</code>
     */
    public Builder clearUpdateLastAccessTime() {
      bitField0_ = (bitField0_ & ~0x00000008);
      updateLastAccessTime_ = true;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.OpenFilePOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.OpenFilePOptions)
  private static final alluxio.grpc.OpenFilePOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.OpenFilePOptions();
  }

  public static alluxio.grpc.OpenFilePOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<OpenFilePOptions>
      PARSER = new com.google.protobuf.AbstractParser<OpenFilePOptions>() {
    public OpenFilePOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new OpenFilePOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<OpenFilePOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<OpenFilePOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.OpenFilePOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


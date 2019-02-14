// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/file_system_master.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.file.SyncPointInfo}
 */
public  final class SyncPointInfo extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.file.SyncPointInfo)
    SyncPointInfoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SyncPointInfo.newBuilder() to construct.
  private SyncPointInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SyncPointInfo() {
    syncPointUri_ = "";
    syncStatus_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SyncPointInfo(
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
            syncPointUri_ = bs;
            break;
          }
          case 16: {
            int rawValue = input.readEnum();
            alluxio.grpc.SyncPointStatus value = alluxio.grpc.SyncPointStatus.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(2, rawValue);
            } else {
              bitField0_ |= 0x00000002;
              syncStatus_ = rawValue;
            }
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
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_SyncPointInfo_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_SyncPointInfo_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.SyncPointInfo.class, alluxio.grpc.SyncPointInfo.Builder.class);
  }

  private int bitField0_;
  public static final int SYNCPOINTURI_FIELD_NUMBER = 1;
  private volatile java.lang.Object syncPointUri_;
  /**
   * <code>optional string syncPointUri = 1;</code>
   */
  public boolean hasSyncPointUri() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional string syncPointUri = 1;</code>
   */
  public java.lang.String getSyncPointUri() {
    java.lang.Object ref = syncPointUri_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        syncPointUri_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string syncPointUri = 1;</code>
   */
  public com.google.protobuf.ByteString
      getSyncPointUriBytes() {
    java.lang.Object ref = syncPointUri_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      syncPointUri_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int SYNCSTATUS_FIELD_NUMBER = 2;
  private int syncStatus_;
  /**
   * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
   */
  public boolean hasSyncStatus() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
   */
  public alluxio.grpc.SyncPointStatus getSyncStatus() {
    alluxio.grpc.SyncPointStatus result = alluxio.grpc.SyncPointStatus.valueOf(syncStatus_);
    return result == null ? alluxio.grpc.SyncPointStatus.Not_Initially_Synced : result;
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, syncPointUri_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeEnum(2, syncStatus_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, syncPointUri_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(2, syncStatus_);
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
    if (!(obj instanceof alluxio.grpc.SyncPointInfo)) {
      return super.equals(obj);
    }
    alluxio.grpc.SyncPointInfo other = (alluxio.grpc.SyncPointInfo) obj;

    boolean result = true;
    result = result && (hasSyncPointUri() == other.hasSyncPointUri());
    if (hasSyncPointUri()) {
      result = result && getSyncPointUri()
          .equals(other.getSyncPointUri());
    }
    result = result && (hasSyncStatus() == other.hasSyncStatus());
    if (hasSyncStatus()) {
      result = result && syncStatus_ == other.syncStatus_;
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
    if (hasSyncPointUri()) {
      hash = (37 * hash) + SYNCPOINTURI_FIELD_NUMBER;
      hash = (53 * hash) + getSyncPointUri().hashCode();
    }
    if (hasSyncStatus()) {
      hash = (37 * hash) + SYNCSTATUS_FIELD_NUMBER;
      hash = (53 * hash) + syncStatus_;
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.SyncPointInfo parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.SyncPointInfo parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.SyncPointInfo parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.SyncPointInfo parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.SyncPointInfo prototype) {
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
   * Protobuf type {@code alluxio.grpc.file.SyncPointInfo}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.file.SyncPointInfo)
      alluxio.grpc.SyncPointInfoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_SyncPointInfo_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_SyncPointInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.SyncPointInfo.class, alluxio.grpc.SyncPointInfo.Builder.class);
    }

    // Construct using alluxio.grpc.SyncPointInfo.newBuilder()
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
      syncPointUri_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      syncStatus_ = 0;
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterProto.internal_static_alluxio_grpc_file_SyncPointInfo_descriptor;
    }

    public alluxio.grpc.SyncPointInfo getDefaultInstanceForType() {
      return alluxio.grpc.SyncPointInfo.getDefaultInstance();
    }

    public alluxio.grpc.SyncPointInfo build() {
      alluxio.grpc.SyncPointInfo result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.SyncPointInfo buildPartial() {
      alluxio.grpc.SyncPointInfo result = new alluxio.grpc.SyncPointInfo(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.syncPointUri_ = syncPointUri_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.syncStatus_ = syncStatus_;
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
      if (other instanceof alluxio.grpc.SyncPointInfo) {
        return mergeFrom((alluxio.grpc.SyncPointInfo)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.SyncPointInfo other) {
      if (other == alluxio.grpc.SyncPointInfo.getDefaultInstance()) return this;
      if (other.hasSyncPointUri()) {
        bitField0_ |= 0x00000001;
        syncPointUri_ = other.syncPointUri_;
        onChanged();
      }
      if (other.hasSyncStatus()) {
        setSyncStatus(other.getSyncStatus());
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
      alluxio.grpc.SyncPointInfo parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.SyncPointInfo) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object syncPointUri_ = "";
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public boolean hasSyncPointUri() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public java.lang.String getSyncPointUri() {
      java.lang.Object ref = syncPointUri_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          syncPointUri_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public com.google.protobuf.ByteString
        getSyncPointUriBytes() {
      java.lang.Object ref = syncPointUri_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        syncPointUri_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public Builder setSyncPointUri(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      syncPointUri_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public Builder clearSyncPointUri() {
      bitField0_ = (bitField0_ & ~0x00000001);
      syncPointUri_ = getDefaultInstance().getSyncPointUri();
      onChanged();
      return this;
    }
    /**
     * <code>optional string syncPointUri = 1;</code>
     */
    public Builder setSyncPointUriBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      syncPointUri_ = value;
      onChanged();
      return this;
    }

    private int syncStatus_ = 0;
    /**
     * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
     */
    public boolean hasSyncStatus() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
     */
    public alluxio.grpc.SyncPointStatus getSyncStatus() {
      alluxio.grpc.SyncPointStatus result = alluxio.grpc.SyncPointStatus.valueOf(syncStatus_);
      return result == null ? alluxio.grpc.SyncPointStatus.Not_Initially_Synced : result;
    }
    /**
     * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
     */
    public Builder setSyncStatus(alluxio.grpc.SyncPointStatus value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000002;
      syncStatus_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.file.SyncPointStatus syncStatus = 2;</code>
     */
    public Builder clearSyncStatus() {
      bitField0_ = (bitField0_ & ~0x00000002);
      syncStatus_ = 0;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.file.SyncPointInfo)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.file.SyncPointInfo)
  private static final alluxio.grpc.SyncPointInfo DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.SyncPointInfo();
  }

  public static alluxio.grpc.SyncPointInfo getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<SyncPointInfo>
      PARSER = new com.google.protobuf.AbstractParser<SyncPointInfo>() {
    public SyncPointInfo parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SyncPointInfo(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SyncPointInfo> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<SyncPointInfo> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.SyncPointInfo getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


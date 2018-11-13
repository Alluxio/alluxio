// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: file_system_master_options.proto

package alluxio.grpc;

/**
 * Protobuf type {@code alluxio.grpc.FileSystemMasterCommonPOptions}
 */
public  final class FileSystemMasterCommonPOptions extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.FileSystemMasterCommonPOptions)
    FileSystemMasterCommonPOptionsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use FileSystemMasterCommonPOptions.newBuilder() to construct.
  private FileSystemMasterCommonPOptions(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private FileSystemMasterCommonPOptions() {
    syncIntervalMs_ = 0L;
    ttl_ = 0L;
    ttlAction_ = 0;
    operationTimeMs_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private FileSystemMasterCommonPOptions(
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
            syncIntervalMs_ = input.readInt64();
            break;
          }
          case 16: {
            bitField0_ |= 0x00000002;
            ttl_ = input.readInt64();
            break;
          }
          case 24: {
            int rawValue = input.readEnum();
            alluxio.grpc.TtlAction value = alluxio.grpc.TtlAction.valueOf(rawValue);
            if (value == null) {
              unknownFields.mergeVarintField(3, rawValue);
            } else {
              bitField0_ |= 0x00000004;
              ttlAction_ = rawValue;
            }
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            operationTimeMs_ = input.readInt64();
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
    return alluxio.grpc.FileSystemMasterOptionsProto.internal_static_alluxio_grpc_FileSystemMasterCommonPOptions_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.FileSystemMasterOptionsProto.internal_static_alluxio_grpc_FileSystemMasterCommonPOptions_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.FileSystemMasterCommonPOptions.class, alluxio.grpc.FileSystemMasterCommonPOptions.Builder.class);
  }

  private int bitField0_;
  public static final int SYNCINTERVALMS_FIELD_NUMBER = 1;
  private long syncIntervalMs_;
  /**
   * <code>optional int64 syncIntervalMs = 1;</code>
   */
  public boolean hasSyncIntervalMs() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional int64 syncIntervalMs = 1;</code>
   */
  public long getSyncIntervalMs() {
    return syncIntervalMs_;
  }

  public static final int TTL_FIELD_NUMBER = 2;
  private long ttl_;
  /**
   * <code>optional int64 ttl = 2;</code>
   */
  public boolean hasTtl() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional int64 ttl = 2;</code>
   */
  public long getTtl() {
    return ttl_;
  }

  public static final int TTLACTION_FIELD_NUMBER = 3;
  private int ttlAction_;
  /**
   * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
   */
  public boolean hasTtlAction() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
   */
  public alluxio.grpc.TtlAction getTtlAction() {
    alluxio.grpc.TtlAction result = alluxio.grpc.TtlAction.valueOf(ttlAction_);
    return result == null ? alluxio.grpc.TtlAction.DELETE : result;
  }

  public static final int OPERATIONTIMEMS_FIELD_NUMBER = 4;
  private long operationTimeMs_;
  /**
   * <code>optional int64 operationTimeMs = 4;</code>
   */
  public boolean hasOperationTimeMs() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional int64 operationTimeMs = 4;</code>
   */
  public long getOperationTimeMs() {
    return operationTimeMs_;
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
      output.writeInt64(1, syncIntervalMs_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeInt64(2, ttl_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeEnum(3, ttlAction_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeInt64(4, operationTimeMs_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, syncIntervalMs_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(2, ttl_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(3, ttlAction_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(4, operationTimeMs_);
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
    if (!(obj instanceof alluxio.grpc.FileSystemMasterCommonPOptions)) {
      return super.equals(obj);
    }
    alluxio.grpc.FileSystemMasterCommonPOptions other = (alluxio.grpc.FileSystemMasterCommonPOptions) obj;

    boolean result = true;
    result = result && (hasSyncIntervalMs() == other.hasSyncIntervalMs());
    if (hasSyncIntervalMs()) {
      result = result && (getSyncIntervalMs()
          == other.getSyncIntervalMs());
    }
    result = result && (hasTtl() == other.hasTtl());
    if (hasTtl()) {
      result = result && (getTtl()
          == other.getTtl());
    }
    result = result && (hasTtlAction() == other.hasTtlAction());
    if (hasTtlAction()) {
      result = result && ttlAction_ == other.ttlAction_;
    }
    result = result && (hasOperationTimeMs() == other.hasOperationTimeMs());
    if (hasOperationTimeMs()) {
      result = result && (getOperationTimeMs()
          == other.getOperationTimeMs());
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
    if (hasSyncIntervalMs()) {
      hash = (37 * hash) + SYNCINTERVALMS_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getSyncIntervalMs());
    }
    if (hasTtl()) {
      hash = (37 * hash) + TTL_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getTtl());
    }
    if (hasTtlAction()) {
      hash = (37 * hash) + TTLACTION_FIELD_NUMBER;
      hash = (53 * hash) + ttlAction_;
    }
    if (hasOperationTimeMs()) {
      hash = (37 * hash) + OPERATIONTIMEMS_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getOperationTimeMs());
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.FileSystemMasterCommonPOptions parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.FileSystemMasterCommonPOptions prototype) {
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
   * Protobuf type {@code alluxio.grpc.FileSystemMasterCommonPOptions}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.FileSystemMasterCommonPOptions)
      alluxio.grpc.FileSystemMasterCommonPOptionsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.FileSystemMasterOptionsProto.internal_static_alluxio_grpc_FileSystemMasterCommonPOptions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.FileSystemMasterOptionsProto.internal_static_alluxio_grpc_FileSystemMasterCommonPOptions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.FileSystemMasterCommonPOptions.class, alluxio.grpc.FileSystemMasterCommonPOptions.Builder.class);
    }

    // Construct using alluxio.grpc.FileSystemMasterCommonPOptions.newBuilder()
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
      syncIntervalMs_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000001);
      ttl_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000002);
      ttlAction_ = 0;
      bitField0_ = (bitField0_ & ~0x00000004);
      operationTimeMs_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.FileSystemMasterOptionsProto.internal_static_alluxio_grpc_FileSystemMasterCommonPOptions_descriptor;
    }

    public alluxio.grpc.FileSystemMasterCommonPOptions getDefaultInstanceForType() {
      return alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance();
    }

    public alluxio.grpc.FileSystemMasterCommonPOptions build() {
      alluxio.grpc.FileSystemMasterCommonPOptions result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.FileSystemMasterCommonPOptions buildPartial() {
      alluxio.grpc.FileSystemMasterCommonPOptions result = new alluxio.grpc.FileSystemMasterCommonPOptions(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.syncIntervalMs_ = syncIntervalMs_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.ttl_ = ttl_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.ttlAction_ = ttlAction_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.operationTimeMs_ = operationTimeMs_;
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
      if (other instanceof alluxio.grpc.FileSystemMasterCommonPOptions) {
        return mergeFrom((alluxio.grpc.FileSystemMasterCommonPOptions)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.FileSystemMasterCommonPOptions other) {
      if (other == alluxio.grpc.FileSystemMasterCommonPOptions.getDefaultInstance()) return this;
      if (other.hasSyncIntervalMs()) {
        setSyncIntervalMs(other.getSyncIntervalMs());
      }
      if (other.hasTtl()) {
        setTtl(other.getTtl());
      }
      if (other.hasTtlAction()) {
        setTtlAction(other.getTtlAction());
      }
      if (other.hasOperationTimeMs()) {
        setOperationTimeMs(other.getOperationTimeMs());
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
      alluxio.grpc.FileSystemMasterCommonPOptions parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.FileSystemMasterCommonPOptions) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private long syncIntervalMs_ ;
    /**
     * <code>optional int64 syncIntervalMs = 1;</code>
     */
    public boolean hasSyncIntervalMs() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int64 syncIntervalMs = 1;</code>
     */
    public long getSyncIntervalMs() {
      return syncIntervalMs_;
    }
    /**
     * <code>optional int64 syncIntervalMs = 1;</code>
     */
    public Builder setSyncIntervalMs(long value) {
      bitField0_ |= 0x00000001;
      syncIntervalMs_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 syncIntervalMs = 1;</code>
     */
    public Builder clearSyncIntervalMs() {
      bitField0_ = (bitField0_ & ~0x00000001);
      syncIntervalMs_ = 0L;
      onChanged();
      return this;
    }

    private long ttl_ ;
    /**
     * <code>optional int64 ttl = 2;</code>
     */
    public boolean hasTtl() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int64 ttl = 2;</code>
     */
    public long getTtl() {
      return ttl_;
    }
    /**
     * <code>optional int64 ttl = 2;</code>
     */
    public Builder setTtl(long value) {
      bitField0_ |= 0x00000002;
      ttl_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 ttl = 2;</code>
     */
    public Builder clearTtl() {
      bitField0_ = (bitField0_ & ~0x00000002);
      ttl_ = 0L;
      onChanged();
      return this;
    }

    private int ttlAction_ = 0;
    /**
     * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
     */
    public boolean hasTtlAction() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
     */
    public alluxio.grpc.TtlAction getTtlAction() {
      alluxio.grpc.TtlAction result = alluxio.grpc.TtlAction.valueOf(ttlAction_);
      return result == null ? alluxio.grpc.TtlAction.DELETE : result;
    }
    /**
     * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
     */
    public Builder setTtlAction(alluxio.grpc.TtlAction value) {
      if (value == null) {
        throw new NullPointerException();
      }
      bitField0_ |= 0x00000004;
      ttlAction_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .alluxio.grpc.TtlAction ttlAction = 3;</code>
     */
    public Builder clearTtlAction() {
      bitField0_ = (bitField0_ & ~0x00000004);
      ttlAction_ = 0;
      onChanged();
      return this;
    }

    private long operationTimeMs_ ;
    /**
     * <code>optional int64 operationTimeMs = 4;</code>
     */
    public boolean hasOperationTimeMs() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int64 operationTimeMs = 4;</code>
     */
    public long getOperationTimeMs() {
      return operationTimeMs_;
    }
    /**
     * <code>optional int64 operationTimeMs = 4;</code>
     */
    public Builder setOperationTimeMs(long value) {
      bitField0_ |= 0x00000008;
      operationTimeMs_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 operationTimeMs = 4;</code>
     */
    public Builder clearOperationTimeMs() {
      bitField0_ = (bitField0_ & ~0x00000008);
      operationTimeMs_ = 0L;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.FileSystemMasterCommonPOptions)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.FileSystemMasterCommonPOptions)
  private static final alluxio.grpc.FileSystemMasterCommonPOptions DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.FileSystemMasterCommonPOptions();
  }

  public static alluxio.grpc.FileSystemMasterCommonPOptions getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<FileSystemMasterCommonPOptions>
      PARSER = new com.google.protobuf.AbstractParser<FileSystemMasterCommonPOptions>() {
    public FileSystemMasterCommonPOptions parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new FileSystemMasterCommonPOptions(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<FileSystemMasterCommonPOptions> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<FileSystemMasterCommonPOptions> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.FileSystemMasterCommonPOptions getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}


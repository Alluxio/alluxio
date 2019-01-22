// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/key_value_master.proto

package alluxio.grpc;

/**
 * <pre>
 **
 * Information about a key-value partition.
 * </pre>
 *
 * Protobuf type {@code alluxio.grpc.keyvalue.PartitionInfo}
 */
public  final class PartitionInfo extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:alluxio.grpc.keyvalue.PartitionInfo)
    PartitionInfoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use PartitionInfo.newBuilder() to construct.
  private PartitionInfo(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private PartitionInfo() {
    keyStart_ = com.google.protobuf.ByteString.EMPTY;
    keyLimit_ = com.google.protobuf.ByteString.EMPTY;
    blockId_ = 0L;
    keyCount_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private PartitionInfo(
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
            bitField0_ |= 0x00000001;
            keyStart_ = input.readBytes();
            break;
          }
          case 18: {
            bitField0_ |= 0x00000002;
            keyLimit_ = input.readBytes();
            break;
          }
          case 24: {
            bitField0_ |= 0x00000004;
            blockId_ = input.readInt64();
            break;
          }
          case 32: {
            bitField0_ |= 0x00000008;
            keyCount_ = input.readInt32();
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
    return alluxio.grpc.KeyValueMasterProto.internal_static_alluxio_grpc_keyvalue_PartitionInfo_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return alluxio.grpc.KeyValueMasterProto.internal_static_alluxio_grpc_keyvalue_PartitionInfo_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            alluxio.grpc.PartitionInfo.class, alluxio.grpc.PartitionInfo.Builder.class);
  }

  private int bitField0_;
  public static final int KEYSTART_FIELD_NUMBER = 1;
  private com.google.protobuf.ByteString keyStart_;
  /**
   * <code>optional bytes keyStart = 1;</code>
   */
  public boolean hasKeyStart() {
    return ((bitField0_ & 0x00000001) == 0x00000001);
  }
  /**
   * <code>optional bytes keyStart = 1;</code>
   */
  public com.google.protobuf.ByteString getKeyStart() {
    return keyStart_;
  }

  public static final int KEYLIMIT_FIELD_NUMBER = 2;
  private com.google.protobuf.ByteString keyLimit_;
  /**
   * <code>optional bytes keyLimit = 2;</code>
   */
  public boolean hasKeyLimit() {
    return ((bitField0_ & 0x00000002) == 0x00000002);
  }
  /**
   * <code>optional bytes keyLimit = 2;</code>
   */
  public com.google.protobuf.ByteString getKeyLimit() {
    return keyLimit_;
  }

  public static final int BLOCKID_FIELD_NUMBER = 3;
  private long blockId_;
  /**
   * <code>optional int64 blockId = 3;</code>
   */
  public boolean hasBlockId() {
    return ((bitField0_ & 0x00000004) == 0x00000004);
  }
  /**
   * <code>optional int64 blockId = 3;</code>
   */
  public long getBlockId() {
    return blockId_;
  }

  public static final int KEYCOUNT_FIELD_NUMBER = 4;
  private int keyCount_;
  /**
   * <code>optional int32 keyCount = 4;</code>
   */
  public boolean hasKeyCount() {
    return ((bitField0_ & 0x00000008) == 0x00000008);
  }
  /**
   * <code>optional int32 keyCount = 4;</code>
   */
  public int getKeyCount() {
    return keyCount_;
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
      output.writeBytes(1, keyStart_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      output.writeBytes(2, keyLimit_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      output.writeInt64(3, blockId_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      output.writeInt32(4, keyCount_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) == 0x00000001)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(1, keyStart_);
    }
    if (((bitField0_ & 0x00000002) == 0x00000002)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(2, keyLimit_);
    }
    if (((bitField0_ & 0x00000004) == 0x00000004)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(3, blockId_);
    }
    if (((bitField0_ & 0x00000008) == 0x00000008)) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(4, keyCount_);
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
    if (!(obj instanceof alluxio.grpc.PartitionInfo)) {
      return super.equals(obj);
    }
    alluxio.grpc.PartitionInfo other = (alluxio.grpc.PartitionInfo) obj;

    boolean result = true;
    result = result && (hasKeyStart() == other.hasKeyStart());
    if (hasKeyStart()) {
      result = result && getKeyStart()
          .equals(other.getKeyStart());
    }
    result = result && (hasKeyLimit() == other.hasKeyLimit());
    if (hasKeyLimit()) {
      result = result && getKeyLimit()
          .equals(other.getKeyLimit());
    }
    result = result && (hasBlockId() == other.hasBlockId());
    if (hasBlockId()) {
      result = result && (getBlockId()
          == other.getBlockId());
    }
    result = result && (hasKeyCount() == other.hasKeyCount());
    if (hasKeyCount()) {
      result = result && (getKeyCount()
          == other.getKeyCount());
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
    if (hasKeyStart()) {
      hash = (37 * hash) + KEYSTART_FIELD_NUMBER;
      hash = (53 * hash) + getKeyStart().hashCode();
    }
    if (hasKeyLimit()) {
      hash = (37 * hash) + KEYLIMIT_FIELD_NUMBER;
      hash = (53 * hash) + getKeyLimit().hashCode();
    }
    if (hasBlockId()) {
      hash = (37 * hash) + BLOCKID_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getBlockId());
    }
    if (hasKeyCount()) {
      hash = (37 * hash) + KEYCOUNT_FIELD_NUMBER;
      hash = (53 * hash) + getKeyCount();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static alluxio.grpc.PartitionInfo parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.PartitionInfo parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PartitionInfo parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static alluxio.grpc.PartitionInfo parseFrom(
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
  public static Builder newBuilder(alluxio.grpc.PartitionInfo prototype) {
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
   * <pre>
   **
   * Information about a key-value partition.
   * </pre>
   *
   * Protobuf type {@code alluxio.grpc.keyvalue.PartitionInfo}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:alluxio.grpc.keyvalue.PartitionInfo)
      alluxio.grpc.PartitionInfoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return alluxio.grpc.KeyValueMasterProto.internal_static_alluxio_grpc_keyvalue_PartitionInfo_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return alluxio.grpc.KeyValueMasterProto.internal_static_alluxio_grpc_keyvalue_PartitionInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              alluxio.grpc.PartitionInfo.class, alluxio.grpc.PartitionInfo.Builder.class);
    }

    // Construct using alluxio.grpc.PartitionInfo.newBuilder()
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
      keyStart_ = com.google.protobuf.ByteString.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      keyLimit_ = com.google.protobuf.ByteString.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000002);
      blockId_ = 0L;
      bitField0_ = (bitField0_ & ~0x00000004);
      keyCount_ = 0;
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return alluxio.grpc.KeyValueMasterProto.internal_static_alluxio_grpc_keyvalue_PartitionInfo_descriptor;
    }

    public alluxio.grpc.PartitionInfo getDefaultInstanceForType() {
      return alluxio.grpc.PartitionInfo.getDefaultInstance();
    }

    public alluxio.grpc.PartitionInfo build() {
      alluxio.grpc.PartitionInfo result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public alluxio.grpc.PartitionInfo buildPartial() {
      alluxio.grpc.PartitionInfo result = new alluxio.grpc.PartitionInfo(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
        to_bitField0_ |= 0x00000001;
      }
      result.keyStart_ = keyStart_;
      if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
        to_bitField0_ |= 0x00000002;
      }
      result.keyLimit_ = keyLimit_;
      if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
        to_bitField0_ |= 0x00000004;
      }
      result.blockId_ = blockId_;
      if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
        to_bitField0_ |= 0x00000008;
      }
      result.keyCount_ = keyCount_;
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
      if (other instanceof alluxio.grpc.PartitionInfo) {
        return mergeFrom((alluxio.grpc.PartitionInfo)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(alluxio.grpc.PartitionInfo other) {
      if (other == alluxio.grpc.PartitionInfo.getDefaultInstance()) return this;
      if (other.hasKeyStart()) {
        setKeyStart(other.getKeyStart());
      }
      if (other.hasKeyLimit()) {
        setKeyLimit(other.getKeyLimit());
      }
      if (other.hasBlockId()) {
        setBlockId(other.getBlockId());
      }
      if (other.hasKeyCount()) {
        setKeyCount(other.getKeyCount());
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
      alluxio.grpc.PartitionInfo parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (alluxio.grpc.PartitionInfo) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.ByteString keyStart_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes keyStart = 1;</code>
     */
    public boolean hasKeyStart() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes keyStart = 1;</code>
     */
    public com.google.protobuf.ByteString getKeyStart() {
      return keyStart_;
    }
    /**
     * <code>optional bytes keyStart = 1;</code>
     */
    public Builder setKeyStart(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      keyStart_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes keyStart = 1;</code>
     */
    public Builder clearKeyStart() {
      bitField0_ = (bitField0_ & ~0x00000001);
      keyStart_ = getDefaultInstance().getKeyStart();
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString keyLimit_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes keyLimit = 2;</code>
     */
    public boolean hasKeyLimit() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional bytes keyLimit = 2;</code>
     */
    public com.google.protobuf.ByteString getKeyLimit() {
      return keyLimit_;
    }
    /**
     * <code>optional bytes keyLimit = 2;</code>
     */
    public Builder setKeyLimit(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      keyLimit_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes keyLimit = 2;</code>
     */
    public Builder clearKeyLimit() {
      bitField0_ = (bitField0_ & ~0x00000002);
      keyLimit_ = getDefaultInstance().getKeyLimit();
      onChanged();
      return this;
    }

    private long blockId_ ;
    /**
     * <code>optional int64 blockId = 3;</code>
     */
    public boolean hasBlockId() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int64 blockId = 3;</code>
     */
    public long getBlockId() {
      return blockId_;
    }
    /**
     * <code>optional int64 blockId = 3;</code>
     */
    public Builder setBlockId(long value) {
      bitField0_ |= 0x00000004;
      blockId_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 blockId = 3;</code>
     */
    public Builder clearBlockId() {
      bitField0_ = (bitField0_ & ~0x00000004);
      blockId_ = 0L;
      onChanged();
      return this;
    }

    private int keyCount_ ;
    /**
     * <code>optional int32 keyCount = 4;</code>
     */
    public boolean hasKeyCount() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int32 keyCount = 4;</code>
     */
    public int getKeyCount() {
      return keyCount_;
    }
    /**
     * <code>optional int32 keyCount = 4;</code>
     */
    public Builder setKeyCount(int value) {
      bitField0_ |= 0x00000008;
      keyCount_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 keyCount = 4;</code>
     */
    public Builder clearKeyCount() {
      bitField0_ = (bitField0_ & ~0x00000008);
      keyCount_ = 0;
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


    // @@protoc_insertion_point(builder_scope:alluxio.grpc.keyvalue.PartitionInfo)
  }

  // @@protoc_insertion_point(class_scope:alluxio.grpc.keyvalue.PartitionInfo)
  private static final alluxio.grpc.PartitionInfo DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new alluxio.grpc.PartitionInfo();
  }

  public static alluxio.grpc.PartitionInfo getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<PartitionInfo>
      PARSER = new com.google.protobuf.AbstractParser<PartitionInfo>() {
    public PartitionInfo parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new PartitionInfo(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<PartitionInfo> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<PartitionInfo> getParserForType() {
    return PARSER;
  }

  public alluxio.grpc.PartitionInfo getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

